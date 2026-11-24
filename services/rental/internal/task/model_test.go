package task

import (
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestTaskRules(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	claim := uuid.New()
	live, expired := now.Add(time.Second), now.Add(-time.Second)
	future, past := now.Add(time.Minute), now.Add(-time.Second)
	task := func(attempts int, lease *time.Time, deadline *time.Time) entry {
		t := entry{kind: Start, status: Pending, schedule: schedule{attempts: attempts, deadline: deadline}}
		if lease != nil {
			t.claimID, t.leaseUntil = &claim, lease
		}
		return t
	}

	if !task(1, &live, &future).owns(claim, now) {
		t.Fatal("live claim must own the task")
	}
	if task(1, &expired, &future).owns(claim, now) || task(1, &live, &past).owns(claim, now) ||
		task(1, &live, &future).owns(uuid.New(), now) {
		t.Fatal("stale claim, deadline or foreign claim must not own the task")
	}

	const maxAttempts = 3
	tests := []struct {
		name string
		task entry
		want Reason
	}{
		{"attempts left", task(2, nil, &future), ""},
		{"last attempt in flight", task(3, &live, &future), ""},
		{"last lease expired", task(3, &expired, &future), MaxAttemptsExceeded},
		{"deadline beats live lease", task(1, &live, &past), DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.task.startClosure(now, maxAttempts); got != tt.want {
				t.Fatalf("reason = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	low := func() float64 { return 0 }
	high := func() float64 { return 0.999999 }
	base, maxDelay := time.Second, 10*time.Second
	if got := backoff(1, base, maxDelay, low); got != 500*time.Millisecond {
		t.Fatalf("attempt 1 low = %s", got)
	}
	if got := backoff(3, base, maxDelay, high); got < 3900*time.Millisecond || got > 4*time.Second {
		t.Fatalf("attempt 3 high = %s", got)
	}
	if got := backoff(50, base, maxDelay, high); got > maxDelay {
		t.Fatalf("cap exceeded: %s", got)
	}
}
