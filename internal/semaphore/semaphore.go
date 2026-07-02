// Package semaphore implements the stick pool: a fixed-capacity counting
// semaphore with a FIFO waiting queue that reports queue position.
//
// A "stick" is one unit of capacity. Acquiring a stick returns a Ticket that
// must be Released when the holder is done. When no stick is free, Acquire
// enqueues the caller and returns a Waiter that exposes its live queue position
// (for backpressure) and a channel that delivers the Ticket once a stick frees
// up. Grants are strict FIFO: a released stick goes to the longest-waiting caller.
//
// The pool is safe for concurrent use.
package semaphore

import (
	"context"
	"sync"
)

// Pool is a counting semaphore with a position-reporting FIFO queue.
type Pool struct {
	mu       sync.Mutex
	capacity int
	free     int
	queue    []*Waiter
}

// New returns a pool with the given capacity (number of sticks). Capacity < 1 is
// clamped to 1.
func New(capacity int) *Pool {
	if capacity < 1 {
		capacity = 1
	}
	return &Pool{capacity: capacity, free: capacity}
}

// Ticket is a held stick. Release returns it to the pool exactly once; further
// Releases are no-ops.
type Ticket struct {
	pool     *Pool
	released bool
	mu       sync.Mutex
}

// Release returns the stick to the pool.
func (t *Ticket) Release() {
	t.mu.Lock()
	if t.released {
		t.mu.Unlock()
		return
	}
	t.released = true
	t.mu.Unlock()
	t.pool.release()
}

// Waiter represents a caller waiting for (or holding) a stick.
type Waiter struct {
	pool    *Pool
	granted chan *Ticket // buffered(1); receives the ticket once a stick is assigned
	done    bool         // granted or cancelled (guarded by pool.mu)
}

// Granted delivers the Ticket once a stick is assigned. It fires at most once.
func (w *Waiter) Granted() <-chan *Ticket { return w.granted }

// Position reports the caller's current place in the queue: 0 means a stick has
// been (or is about to be) granted; N>=1 means N callers are ahead.
func (w *Waiter) Position() int {
	w.pool.mu.Lock()
	defer w.pool.mu.Unlock()
	for i, q := range w.pool.queue {
		if q == w {
			return i + 1
		}
	}
	return 0
}

// Cancel abandons the wait. It is a no-op if the stick was already granted; in
// that case the caller must Release the delivered Ticket instead. Safe to defer.
func (w *Waiter) Cancel() {
	w.pool.mu.Lock()
	defer w.pool.mu.Unlock()
	if w.done {
		return
	}
	for i, q := range w.pool.queue {
		if q == w {
			w.pool.queue = append(w.pool.queue[:i], w.pool.queue[i+1:]...)
			w.done = true
			return
		}
	}
}

// Acquire requests a stick. If one is free it is granted immediately (the
// returned Waiter's Granted channel is already loaded). Otherwise the caller is
// enqueued; watch Granted for the Ticket and Position for backpressure, and call
// Cancel (or let ctx cancellation be handled by the caller) to give up.
//
// Acquire itself never blocks. ctx is retained only so a cancelled context can
// be observed by the caller alongside Granted; the pool does not auto-cancel.
func (p *Pool) Acquire(ctx context.Context) *Waiter {
	w := &Waiter{pool: p, granted: make(chan *Ticket, 1)}
	p.mu.Lock()
	if p.free > 0 && len(p.queue) == 0 {
		p.free--
		p.mu.Unlock()
		w.done = true
		w.granted <- &Ticket{pool: p}
		return w
	}
	p.queue = append(p.queue, w)
	p.mu.Unlock()
	return w
}

// release returns a stick, handing it to the head waiter if any.
func (p *Pool) release() {
	p.mu.Lock()
	if len(p.queue) > 0 {
		w := p.queue[0]
		p.queue = p.queue[1:]
		w.done = true
		p.mu.Unlock()
		w.granted <- &Ticket{pool: p} // hand-off: free count unchanged
		return
	}
	if p.free < p.capacity {
		p.free++
	}
	p.mu.Unlock()
}

// Stats is a point-in-time snapshot of pool pressure.
type Stats struct {
	Total      int
	InUse      int
	QueueDepth int
}

// Stats returns current utilization.
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{Total: p.capacity, InUse: p.capacity - p.free, QueueDepth: len(p.queue)}
}
