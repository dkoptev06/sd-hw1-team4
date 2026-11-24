package main

import (
	"math/rand/v2"
	"sync"
)

// fleetPool pairs available scooters with independent users so that at most one
// active trip uses any scooter or user at a time, while breaking modular
// correlations (such as zone_id vs has_subscription in Source's default catalog).
type fleetPool struct {
	mu            sync.Mutex
	scooters      []int
	sHead, sCount int
	users         []int
	rng           *rand.Rand
}

func newFleetPool(size int, seed int64) *fleetPool {
	scooters := make([]int, size)
	users := make([]int, size)
	for i := range size {
		scooters[i] = i + 1
		users[i] = i + 1
	}
	return &fleetPool{
		scooters: scooters,
		sCount:   size,
		users:    users,
		rng:      rand.New(rand.NewPCG(uint64(seed), 0x5f3759df)),
	}
}

func (p *fleetPool) acquire() (scooterID, userID int, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sCount == 0 || len(p.users) == 0 {
		return 0, 0, false
	}
	scooterID = p.scooters[p.sHead]
	p.sHead = (p.sHead + 1) % len(p.scooters)
	p.sCount--

	idx := p.rng.IntN(len(p.users))
	userID = p.users[idx]
	last := len(p.users) - 1
	p.users[idx] = p.users[last]
	p.users = p.users[:last]
	return scooterID, userID, true
}

func (p *fleetPool) release(scooterID, userID int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scooters[(p.sHead+p.sCount)%len(p.scooters)] = scooterID
	p.sCount++
	p.users = append(p.users, userID)
}
