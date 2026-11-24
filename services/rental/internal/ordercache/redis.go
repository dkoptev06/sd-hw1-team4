package ordercache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RemoteCache is the shared L2 cache interface (implemented by RedisClient).
type RemoteCache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	SetPX(ctx context.Context, key string, val []byte, ttl time.Duration) error
	Close() error
}

// RedisClient is a minimal, pooled RESP2 client for GET and SET with bounded
// per-call deadlines and zero external dependencies.
type RedisClient struct {
	addr     string
	password string
	db       int
	timeout  time.Duration

	mu     sync.Mutex
	closed bool
	idle   chan *redisConn
	slots  chan struct{}
}

type redisConn struct {
	net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

// NewRedisClient parses a redis://[:password@]host[:port][/db] URL.
func NewRedisClient(rawURL string, poolSize int, timeout time.Duration) (*RedisClient, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, errors.New("invalid REDIS_URL")
	}
	if u.Scheme != "redis" || u.Host == "" {
		return nil, errors.New("REDIS_URL must use redis://host[:port][/db]")
	}
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	addr := net.JoinHostPort(u.Hostname(), port)
	var password string
	if u.User != nil {
		password, _ = u.User.Password()
	}
	db := 0
	if path := strings.TrimPrefix(u.Path, "/"); path != "" {
		db, err = strconv.Atoi(path)
		if err != nil || db < 0 {
			return nil, fmt.Errorf("invalid Redis DB %q", path)
		}
	}
	if poolSize <= 0 {
		poolSize = 32
	}
	if timeout <= 0 {
		timeout = 50 * time.Millisecond
	}
	return &RedisClient{
		addr:     addr,
		password: password,
		db:       db,
		timeout:  timeout,
		idle:     make(chan *redisConn, poolSize),
		slots:    make(chan struct{}, poolSize),
	}, nil
}

func (c *RedisClient) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return c.do(ctx, "GET", key)
}

func (c *RedisClient) SetPX(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	ms := ttl.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	_, _, err := c.do(ctx, "SET", key, string(val), "PX", strconv.FormatInt(ms, 10))
	return err
}

func (c *RedisClient) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.idle)
	c.mu.Unlock()
	for rc := range c.idle {
		_ = rc.Close()
	}
	return nil
}

func (c *RedisClient) do(ctx context.Context, args ...string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	rc, reused, err := c.acquire(ctx)
	if err != nil {
		return nil, false, err
	}
	value, found, err := c.exec(ctx, rc, args...)
	if err != nil && reused && ctx.Err() == nil {
		// Redis may have restarted while this socket was idle. Bypass the
		// rest of the idle pool when probing for recovery.
		_ = rc.Close()
		rc, err = c.dial(ctx)
		if err != nil {
			return nil, false, err
		}
		value, found, err = c.exec(ctx, rc, args...)
	}
	if err != nil {
		_ = rc.Close()
		return nil, false, err
	}
	c.release(rc)
	return value, found, nil
}

func (c *RedisClient) acquire(ctx context.Context) (*redisConn, bool, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, false, errors.New("redis client closed")
	}
	select {
	case rc, ok := <-c.idle:
		if ok && rc != nil {
			return rc, true, nil
		}
	default:
	}
	rc, err := c.dial(ctx)
	return rc, false, err
}

func (c *RedisClient) release(rc *redisConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = rc.Close()
		return
	}
	select {
	case c.idle <- rc:
	default:
		_ = rc.Close()
	}
}

func (c *RedisClient) dial(ctx context.Context) (*redisConn, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	rc := &redisConn{Conn: raw, r: bufio.NewReaderSize(raw, 4096), w: bufio.NewWriterSize(raw, 4096)}
	if c.password != "" {
		if _, _, err := c.exec(ctx, rc, "AUTH", c.password); err != nil {
			_ = rc.Close()
			return nil, err
		}
	}
	if c.db != 0 {
		if _, _, err := c.exec(ctx, rc, "SELECT", strconv.Itoa(c.db)); err != nil {
			_ = rc.Close()
			return nil, err
		}
	}
	return rc, nil
}

func (c *RedisClient) deadline(ctx context.Context) time.Time {
	d := time.Now().Add(c.timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(d) {
		return ctxDeadline
	}
	return d
}

func (c *RedisClient) exec(ctx context.Context, rc *redisConn, args ...string) ([]byte, bool, error) {
	if err := rc.SetDeadline(c.deadline(ctx)); err != nil {
		return nil, false, err
	}
	if err := writeRESPArray(rc.w, args); err != nil {
		return nil, false, err
	}
	if err := rc.w.Flush(); err != nil {
		return nil, false, err
	}
	return readRESPReply(rc.r)
}

func writeRESPArray(w *bufio.Writer, args []string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, a := range args {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(a), a); err != nil {
			return err
		}
	}
	return nil
}

func readRESPReply(r *bufio.Reader) ([]byte, bool, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, false, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return nil, false, fmt.Errorf("redis: malformed reply %q", line)
	}
	prefix := line[0]
	payload := line[1 : len(line)-2]
	switch prefix {
	case '+':
		return []byte(payload), true, nil
	case ':':
		return []byte(payload), true, nil
	case '-':
		return nil, false, fmt.Errorf("redis error: %s", payload)
	case '$':
		n, err := strconv.Atoi(payload)
		if err != nil {
			return nil, false, fmt.Errorf("redis: invalid bulk length %q", payload)
		}
		if n == -1 {
			return nil, false, nil
		}
		if n < 0 || n > 1<<20 {
			return nil, false, errors.New("redis: bulk length out of bounds")
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, false, err
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return nil, false, errors.New("redis: missing CRLF after bulk string")
		}
		return buf[:n], true, nil
	default:
		return nil, false, fmt.Errorf("redis: unsupported reply prefix %q", prefix)
	}
}
