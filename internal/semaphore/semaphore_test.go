package semaphore

import (
	"context"
	"testing"
	"time"
)

func TestImmediateGrantWithinCapacity(t *testing.T) {
	p := New(2)
	ctx := context.Background()
	w1 := p.Acquire(ctx)
	w2 := p.Acquire(ctx)
	for _, w := range []*Waiter{w1, w2} {
		select {
		case <-w.Granted():
		default:
			t.Fatal("expected immediate grant within capacity")
		}
	}
	if got := p.Stats().InUse; got != 2 {
		t.Fatalf("InUse = %d, want 2", got)
	}
}

func TestQueueAndFIFOHandoff(t *testing.T) {
	p := New(1)
	ctx := context.Background()

	w1 := p.Acquire(ctx)
	t1 := mustGrant(t, w1)

	// Two more queue behind the held stick.
	w2 := p.Acquire(ctx)
	w3 := p.Acquire(ctx)
	if pos := w2.Position(); pos != 1 {
		t.Fatalf("w2 position = %d, want 1", pos)
	}
	if pos := w3.Position(); pos != 2 {
		t.Fatalf("w3 position = %d, want 2", pos)
	}
	if got := p.Stats().QueueDepth; got != 2 {
		t.Fatalf("QueueDepth = %d, want 2", got)
	}

	// Release -> the head (w2) gets it, FIFO.
	t1.Release()
	t2 := mustGrant(t, w2)
	if pos := w3.Position(); pos != 1 {
		t.Fatalf("after handoff w3 position = %d, want 1", pos)
	}

	t2.Release()
	mustGrant(t, w3)
}

func TestCancelRemovesFromQueue(t *testing.T) {
	p := New(1)
	ctx := context.Background()
	hold := mustGrant(t, p.Acquire(ctx))

	w2 := p.Acquire(ctx)
	w3 := p.Acquire(ctx)
	w2.Cancel() // give up while queued

	if pos := w3.Position(); pos != 1 {
		t.Fatalf("after w2 cancel, w3 position = %d, want 1", pos)
	}
	hold.Release()
	mustGrant(t, w3) // stick skips the cancelled w2 and goes to w3
}

func TestReleaseIsIdempotent(t *testing.T) {
	p := New(1)
	tk := mustGrant(t, p.Acquire(context.Background()))
	tk.Release()
	tk.Release() // must not double-free capacity
	if got := p.Stats().InUse; got != 0 {
		t.Fatalf("InUse = %d, want 0", got)
	}
}

func mustGrant(t *testing.T, w *Waiter) *Ticket {
	t.Helper()
	select {
	case tk := <-w.Granted():
		return tk
	case <-time.After(time.Second):
		t.Fatal("expected a grant")
		return nil
	}
}
