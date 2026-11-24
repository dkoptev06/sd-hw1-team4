// Package ledger owns the in-memory state of Payment operations.
package ledger

import (
	"math/big"
	"sync"
	"time"
)

type Service struct {
	mu           sync.Mutex
	ops          map[operationID]*operation
	now          func() time.Time
	settleResult string
}

func NewService(now func() time.Time, failSettle bool) *Service {
	result := Success
	if failSettle {
		result = Failed
	}
	return &Service{ops: make(map[operationID]*operation), now: now, settleResult: result}
}

// operation returns the operation for a command while the service mutex is held.
func (s *Service) operation(id operationID) *operation {
	op, ok := s.ops[id]
	if !ok {
		op = new(operation)
		s.ops[id] = op
	}
	return op
}

func (s *Service) Hold(id, userID string, amount int64, expiresAt time.Time) (string, error) {
	cmd, err := newHoldCommand(id, userID, amount, expiresAt)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.operation(cmd.id).applyHold(cmd, s.now)
}

func (s *Service) Settle(id, userID string, total *big.Int) (string, error) {
	cmd, err := newSettleCommand(id, userID, total)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.operation(cmd.id).applySettle(cmd, s.settleResult)
}

func (s *Service) Void(id string) (string, error) {
	key, err := newOperationID(id)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.operation(key).applyVoid()
}

func (s *Service) Status(id string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	op, ok := s.ops[operationID(id)]
	if !ok {
		return "", errNotFound
	}
	return op.status(s.now)
}
