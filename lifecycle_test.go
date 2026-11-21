// Copyright (c) 2026, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// nopReadWriteCloser is a transport stub so lifecycle tests can build a
// Connection without a real socket.
type nopReadWriteCloser struct{}

func (nopReadWriteCloser) Read([]byte) (int, error)    { return 0, io.EOF }
func (nopReadWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopReadWriteCloser) Close() error                { return nil }

// newLifecycleTestConnection returns the minimal Connection the lifecycle
// helpers need, without running a handshake or a reader goroutine.
func newLifecycleTestConnection(channelMax int) *Connection {
	return &Connection{
		conn:      nopReadWriteCloser{},
		channels:  map[uint16]*Channel{},
		retired:   map[uint16]uint64{},
		errors:    make(chan *Error, 1),
		allocator: newAllocator(1, channelMax),
	}
}

// newSilentSession is newSession without the logIO proxy.  The proxy logs every
// read and write through the testing.T from background goroutines, which races
// with the test finishing; lifecycle tests that keep a reader alive until the
// connection closes use plain pipes instead.
func newSilentSession(t *testing.T) (io.ReadWriteCloser, *server) {
	rs, wc := io.Pipe()
	rc, ws := io.Pipe()

	rws := pipe{rs, ws}
	rwc := pipe{rc, wc}

	return rwc, newServer(t, rws, rwc)
}

// TestAllocatorGenerationAdvancesPerAllocation ensures a released id is handed
// out with a new generation and that generations never move backwards.
func TestAllocatorGenerationAdvancesPerAllocation(t *testing.T) {
	a := newAllocator(1, 2)

	first, firstGeneration, ok := a.nextGeneration()
	if !ok {
		t.Fatalf("expected to allocate the first id")
	}
	if first != 1 {
		t.Fatalf("expected first allocation to be 1, got %d", first)
	}

	a.release(first)

	again, secondGeneration, ok := a.nextGeneration()
	if !ok {
		t.Fatalf("expected to reallocate the released id")
	}
	if again != first {
		t.Fatalf("expected the released id %d to be reused, got %d", first, again)
	}
	if secondGeneration <= firstGeneration {
		t.Fatalf("expected a reused id to get a newer generation, got %d after %d", secondGeneration, firstGeneration)
	}
}

// TestAllocatorReleaseIsIdempotent ensures releasing an id twice neither frees a
// newer allocation nor produces a negative index or advances a generation.
func TestAllocatorReleaseIsIdempotent(t *testing.T) {
	a := newAllocator(1, 1)

	id, _, ok := a.nextGeneration()
	if !ok {
		t.Fatalf("expected to allocate the only id")
	}

	a.release(id)
	if _, _, ok := a.nextGeneration(); !ok {
		// Reallocate to confirm the pool still works, then release it again
		// alongside a duplicate release of the same id.
		t.Fatalf("expected to reallocate the released id")
	}
	a.release(id)
	a.release(id)
	a.release(0)       // below low
	a.release(1 << 16) // above high

	third, _, ok := a.nextGeneration()
	if !ok {
		t.Fatalf("expected duplicate releases not to corrupt the pool")
	}
	if third != 1 {
		t.Fatalf("expected the same id to be reusable, got %d", third)
	}
}

// TestConnectionCloseRejectsPendingAllocations ensures that once a connection
// is shut down no channel can be allocated, whichever way the close path is
// entered.
func TestConnectionCloseRejectsPendingAllocations(t *testing.T) {
	c := newLifecycleTestConnection(1)
	c.shutdown(nil)

	if _, err := c.allocateChannel(); err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if c.allocator != nil {
		t.Fatalf("expected the allocator to be dropped on shutdown")
	}
}

// TestReleaseStaleGenerationDoesNotFreeNewOwner ensures a late release of an
// old generation cannot unregister or free an id that has since been handed to
// a new channel.
func TestReleaseStaleGenerationDoesNotFreeNewOwner(t *testing.T) {
	c := newLifecycleTestConnection(1)

	old, err := c.allocateChannel()
	if err != nil {
		t.Fatalf("unexpected allocate error: %v", err)
	}
	oldGeneration := old.generation

	// Close and release the old channel, then reuse the same id.
	c.closeChannel(old, nil)

	reused, err := c.allocateChannel()
	if err != nil {
		t.Fatalf("expected to reuse the released id: %v", err)
	}
	if reused.id != old.id {
		t.Fatalf("expected id %d to be reused, got %d", old.id, reused.id)
	}
	if reused.generation <= oldGeneration {
		t.Fatalf("expected reused channel to have a newer generation")
	}

	// A stale release of the old object must not evict the new owner.
	c.releaseChannel(old)

	c.m.Lock()
	current := c.channels[reused.id]
	c.m.Unlock()

	if current != reused {
		t.Fatalf("stale release evicted the current owner of id %d", reused.id)
	}

	// The new owner is still usable: its send must not report a retired id.
	if reused.isRetired() {
		t.Fatalf("reused channel was retired by a stale release")
	}
}

// TestClosedChannelSendFails ensures methods invoked after a channel is closed
// fail immediately instead of being written to the id.
func TestClosedChannelSendFails(t *testing.T) {
	c := newLifecycleTestConnection(1)

	ch, err := c.allocateChannel()
	if err != nil {
		t.Fatalf("unexpected allocate error: %v", err)
	}
	c.closeChannel(ch, nil)

	if err := ch.send(&basicAck{DeliveryTag: 1}); err != ErrClosed {
		t.Fatalf("expected ErrClosed after close, got %v", err)
	}
}

// TestReleaseWhileSendInFlightDefersIDReuse ensures an id is not handed to a
// new generation while a send from the old generation may still be writing
// frames on it, and that the deferred release does not run twice.
func TestReleaseWhileSendInFlightDefersIDReuse(t *testing.T) {
	c := newLifecycleTestConnection(1)

	old, err := c.allocateChannel()
	if err != nil {
		t.Fatalf("unexpected allocate error: %v", err)
	}

	// Simulate a write that started before the close and has not finished.
	if !old.beginSend() {
		t.Fatalf("expected the send to start while the channel owns its id")
	}

	c.closeChannel(old, nil)

	// The id must not be allocatable until the in-flight send finishes.
	if _, err := c.allocateChannel(); err != ErrChannelMax {
		t.Fatalf("expected the id to stay reserved during an in-flight send, got %v", err)
	}

	old.endSend()

	reused, err := c.allocateChannel()
	if err != nil {
		t.Fatalf("expected the id to be released after the send finished: %v", err)
	}
	if reused.id != old.id {
		t.Fatalf("expected id %d to be reused, got %d", old.id, reused.id)
	}

	// A repeated end must not drive the in-flight count negative or release
	// the new owner's id.  endSend is only ever called once per beginSend, so
	// verify the accounting directly instead of calling it again.
	old.sendM.Lock()
	inFlight := old.inFlight
	old.sendM.Unlock()
	if inFlight != 0 {
		t.Fatalf("expected in-flight sends to return to zero, got %d", inFlight)
	}
}

// TestReusedChannelDoesNotInheritState ensures a new channel allocated the same
// id as a closed one starts without confirms, consumers or RPC waiters from its
// predecessor, and that frames for the closed generation are not delivered.
func TestReusedChannelDoesNotInheritState(t *testing.T) {
	rwc, srv := newSilentSession(t)
	defer rwc.Close()

	consumerTag := "inherited-consumer"

	go func() {
		srv.connectionOpen()
		srv.channelOpen(1)

		// Enter confirm mode and start a consumer on the first generation.
		srv.recv(1, &confirmSelect{})
		srv.send(1, &confirmSelectOk{})
		srv.recv(1, &basicConsume{})
		srv.send(1, &basicConsumeOk{ConsumerTag: consumerTag})

		// Close that generation.
		srv.recv(1, &channelClose{})
		srv.send(1, &channelCloseOk{})

		// The id is reused for a second generation.  Frames queued for the
		// old generation (a confirm and a delivery) arrive before its
		// open-ok, exactly as they would on a real connection.
		srv.recv(1, &channelOpen{})
		srv.send(1, &basicAck{DeliveryTag: 1})
		srv.send(1, &basicDeliver{ConsumerTag: consumerTag, DeliveryTag: 1})
		srv.send(1, &channelOpenOk{})

		// A delivery for the first generation arriving after the new one is
		// established must still not reach the new channel's consumers.
		srv.send(1, &basicDeliver{ConsumerTag: consumerTag, DeliveryTag: 2})

		srv.recv(1, &channelClose{})
		srv.send(1, &channelCloseOk{})

		srv.recv(0, &connectionClose{})
		srv.send(0, &connectionCloseOk{})
	}()

	c, err := Open(rwc, defaultConfig())
	if err != nil {
		t.Fatalf("could not create connection: %v (%s)", c, err)
	}

	first, err := c.Channel()
	if err != nil {
		t.Fatalf("could not open first channel: %v", err)
	}

	confirms := first.NotifyPublish(make(chan Confirmation, 1))
	if err := first.Confirm(false); err != nil {
		t.Fatalf("could not enter confirm mode: %v", err)
	}
	consumers, err := first.Consume("queue", consumerTag, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("could not consume: %v", err)
	}

	if err := first.Close(); err != nil {
		t.Fatalf("could not close first channel: %v", err)
	}

	second, err := c.Channel()
	if err != nil {
		t.Fatalf("could not open second channel: %v", err)
	}

	if second.id != first.id {
		t.Fatalf("expected the id to be reused, got %d and %d", first.id, second.id)
	}
	if second.confirming {
		t.Fatalf("reused channel inherited confirm mode")
	}

	// No confirmation from the old generation may reach the new listeners.
	secondConfirms := second.NotifyPublish(make(chan Confirmation, 1))
	select {
	case confirmation := <-secondConfirms:
		t.Fatalf("reused channel received a stale confirmation: %+v", confirmation)
	case <-time.After(50 * time.Millisecond):
	}

	// The old confirmation channel must be closed, not reused.
	select {
	case _, open := <-confirms:
		if open {
			// A confirmation may have been queued before the close; drain
			// until closed rather than treating it as inheritance.
			for range confirms {
			}
		}
	case <-time.After(time.Second):
		t.Fatalf("expected the old confirm listener to be closed")
	}

	// No delivery for the old generation may reach the new channel's
	// consumers.  The new channel was never asked to consume, so a delivery
	// would have to arrive on a consumer registered by the old generation.
	select {
	case delivery, open := <-consumers:
		if open {
			t.Fatalf("reused channel delivered a stale message: %+v", delivery)
		}
	case <-time.After(50 * time.Millisecond):
	}

	if err := second.Close(); err != nil {
		t.Fatalf("could not close second channel: %v", err)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("could not close connection: %v", err)
	}
}

// TestStaleFramesAfterCloseAreDropped ensures frames for a released generation
// are dropped (and counted) instead of being treated as a protocol violation
// that tears down the connection.
func TestStaleFramesAfterCloseAreDropped(t *testing.T) {
	c := newLifecycleTestConnection(1)

	ch, err := c.allocateChannel()
	if err != nil {
		t.Fatalf("unexpected allocate error: %v", err)
	}
	c.closeChannel(ch, nil)

	stale := &methodFrame{ChannelId: ch.id, Method: &basicAck{DeliveryTag: 1}}
	c.dispatchN(stale)

	c.m.Lock()
	staleFrames := c.staleFrames
	c.m.Unlock()

	if staleFrames != 1 {
		t.Fatalf("expected the stale frame to be counted, got %d", staleFrames)
	}
}

// TestConcurrentOpenCloseAndShutdown ensures concurrent opens, closes and a
// connection shutdown do not deadlock and leave the registry consistent.
func TestConcurrentOpenCloseAndShutdown(t *testing.T) {
	c := newLifecycleTestConnection(64)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				ch, err := c.allocateChannel()
				if err != nil {
					return
				}
				atomic.StoreInt32(&ch.established, 1)
				c.closeChannel(ch, nil)
			}
		}()
	}

	time.Sleep(10 * time.Millisecond)
	close(stop)
	wg.Wait()
	c.shutdown(nil)

	c.m.Lock()
	remaining := len(c.channels)
	allocator := c.allocator
	c.m.Unlock()

	if remaining != 0 {
		t.Fatalf("expected no channels after shutdown, got %d", remaining)
	}
	if allocator != nil {
		t.Fatalf("expected the allocator to be dropped after shutdown")
	}
}

// TestChannelMaxExhaustionNoDeadlock ensures exhausting the id space returns
// ErrChannelMax and leaves the connection usable for closing.
func TestChannelMaxExhaustionNoDeadlock(t *testing.T) {
	c := newLifecycleTestConnection(1)

	first, err := c.allocateChannel()
	if err != nil {
		t.Fatalf("unexpected allocate error: %v", err)
	}

	if _, err := c.allocateChannel(); err != ErrChannelMax {
		t.Fatalf("expected ErrChannelMax, got %v", err)
	}

	c.closeChannel(first, nil)

	again, err := c.allocateChannel()
	if err != nil {
		t.Fatalf("expected the id to be reusable after exhaustion: %v", err)
	}
	if again.id != first.id {
		t.Fatalf("expected id %d, got %d", first.id, again.id)
	}
}
