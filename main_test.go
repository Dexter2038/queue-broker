package main

import (
	"fmt"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// resetQueues is mandatory before each test.
func resetQueues() {
	queuesMu.Lock()
	queues = make(map[string]*MessageQueue)
	queuesMu.Unlock()
}

// Basic PUT / GET tests (already passed earlier).
func TestPutGetBasic(t *testing.T) {
	resetQueues()
	// PUT
	putReq := httptest.NewRequest("PUT", "/q?v=hello", nil)
	putRec := httptest.NewRecorder()
	handler(putRec, putReq)
	if putRec.Code != 200 {
		t.Fatalf("PUT failed: %d", putRec.Code)
	}

	// GET
	getReq := httptest.NewRequest("GET", "/q", nil)
	getRec := httptest.NewRecorder()
	handler(getRec, getReq)
	if getRec.Code != 200 || getRec.Body.String() != "hello" {
		t.Fatalf("GET failed: %d, body: %q", getRec.Code, getRec.Body.String())
	}

	// second GET → 404
	getReq2 := httptest.NewRequest("GET", "/q", nil)
	getRec2 := httptest.NewRecorder()
	handler(getRec2, getReq2)
	if getRec2.Code != 404 {
		t.Fatalf("second GET should be 404, got %d", getRec2.Code)
	}
}

// Missing v parameter → 400
func TestPutMissingV(t *testing.T) {
	resetQueues()
	req := httptest.NewRequest("PUT", "/q", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != 400 {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

// Timeout on empty queue must block for the given duration, then return 404.
func TestTimeoutExpires(t *testing.T) {
	resetQueues()
	req := httptest.NewRequest("GET", "/q?timeout=1", nil)
	rec := httptest.NewRecorder()

	start := time.Now()
	handler(rec, req)
	elapsed := time.Since(start)

	if rec.Code != 404 {
		t.Errorf("expected 404, got %d", rec.Code)
	}
	if elapsed < 900*time.Millisecond {
		t.Errorf("expected to wait ~1s, waited %v", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("waited too long: %v", elapsed)
	}
}

// Timeout interrupted by a message: must return ASAP.
func TestTimeoutInterrupted(t *testing.T) {
	resetQueues()

	// Put a message after 200ms in a goroutine.
	go func() {
		time.Sleep(200 * time.Millisecond)
		putReq := httptest.NewRequest("PUT", "/q?v=early", nil)
		putRec := httptest.NewRecorder()
		handler(putRec, putReq) // This modifies shared state, it's fine.
	}()

	req := httptest.NewRequest("GET", "/q?timeout=2", nil)
	rec := httptest.NewRecorder()
	start := time.Now()
	handler(rec, req)
	elapsed := time.Since(start)

	if rec.Code != 200 {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "early" {
		t.Errorf("expected 'early', got %q", rec.Body.String())
	}
	if elapsed > 1*time.Second {
		t.Errorf("should have returned quickly, but took %v", elapsed)
	}
}

// FIFO order: enqueue A, B, C; dequeue must return A, B, C.
func TestFIFOOrder(t *testing.T) {
	resetQueues()
	for _, msg := range []string{"first", "second", "third"} {
		req := httptest.NewRequest("PUT", "/q?v="+msg, nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
	}

	for _, expected := range []string{"first", "second", "third"} {
		req := httptest.NewRequest("GET", "/q", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != 200 || rec.Body.String() != expected {
			t.Errorf("expected %q, got %q (code %d)", expected, rec.Body.String(), rec.Code)
		}
	}

	// Queue must be empty now.
	req := httptest.NewRequest("GET", "/q", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != 404 {
		t.Errorf("expected 404 after draining, got %d", rec.Code)
	}
}

func TestWaiterOrdering(t *testing.T) {
	resetQueues()

	var mu sync.Mutex
	results := make([]string, 5) // index = waiter id
	gates := make([]chan struct{}, 5)
	for i := 0; i < 5; i++ {
		gates[i] = make(chan struct{})
	}

	var wg sync.WaitGroup
	// Start 5 waiters; each will block on its gate before calling the handler.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-gates[id] // wait until main goroutine opens this gate
			req := httptest.NewRequest("GET", "/q?timeout=5", nil)
			rec := httptest.NewRecorder()
			handler(rec, req)
			if rec.Code == 200 {
				mu.Lock()
				results[id] = rec.Body.String()
				mu.Unlock()
			}
		}(i)
	}

	// Let them start and block on the gates.
	time.Sleep(10 * time.Millisecond)

	// Open gates sequentially: waiter 0 enters first, then 1, etc.
	for i := 0; i < 5; i++ {
		close(gates[i])
		// Give the goroutine enough time to call handler and register itself.
		time.Sleep(5 * time.Millisecond)
	}

	// Now all waiters should be registered in order. Deliver messages.
	for _, msg := range []string{"msg1", "msg2", "msg3", "msg4", "msg5"} {
		putReq := httptest.NewRequest("PUT", "/q?v="+msg, nil)
		putRec := httptest.NewRecorder()
		handler(putRec, putReq)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for i, expected := range []string{"msg1", "msg2", "msg3", "msg4", "msg5"} {
		if results[i] != expected {
			t.Errorf("waiter order violation: position %d expected %q, got %q", i, expected, results[i])
		}
	}
}

func TestConcurrentPutGetNoTimeout(t *testing.T) {
	resetQueues()
	const numMessages = 1000
	var received atomic.Int64
	var wg sync.WaitGroup

	// Start consumers.
	consumerCount := 10
	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for received.Load() < numMessages {
				req := httptest.NewRequest("GET", "/q", nil)
				rec := httptest.NewRecorder()
				handler(rec, req)
				if rec.Code == 200 {
					received.Add(1)
				}
			}
		}()
	}

	// Start producers.
	producerCount := 10
	for i := 0; i < producerCount; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for j := 0; j < numMessages/producerCount; j++ {
				req := httptest.NewRequest("PUT", "/q?v=x", nil)
				rec := httptest.NewRecorder()
				handler(rec, req)
				if rec.Code != 200 {
					t.Errorf("PUT failed with %d", rec.Code)
				}
			}
		}(i)
	}

	wg.Wait()
	if received.Load() != numMessages {
		t.Errorf("expected %d messages received, got %d", numMessages, received.Load())
	}
}

func TestConcurrentTimeoutMix(t *testing.T) {
	resetQueues()
	const totalMessages = 500
	var sent atomic.Int64
	var received atomic.Int64
	var producersDone atomic.Bool

	var prodWg sync.WaitGroup
	var consWg sync.WaitGroup

	// Consumers
	for i := 0; i < 20; i++ {
		consWg.Add(1)
		go func() {
			defer consWg.Done()
			for {
				if producersDone.Load() && received.Load() >= int64(totalMessages) {
					return
				}
				timeout := 0
				if i%2 == 0 {
					timeout = 1
				}
				req := httptest.NewRequest("GET", "/q?timeout="+strconv.Itoa(timeout), nil)
				rec := httptest.NewRecorder()
				handler(rec, req)
				if rec.Code == 200 {
					received.Add(1)
				}
			}
		}()
	}

	// Producers
	for i := 0; i < 5; i++ {
		prodWg.Add(1)
		go func() {
			defer prodWg.Done()
			for j := 0; j < totalMessages/5; j++ {
				time.Sleep(time.Millisecond * time.Duration(1+j%10))
				req := httptest.NewRequest("PUT", "/q?v=payload", nil)
				rec := httptest.NewRecorder()
				handler(rec, req)
				if rec.Code == 200 {
					sent.Add(1)
				}
			}
		}()
	}

	prodWg.Wait()
	producersDone.Store(true)

	// Wait for consumers to finish draining the queue.
	consWg.Wait()

	if sent.Load() != totalMessages {
		t.Errorf("sent %d messages instead of %d", sent.Load(), totalMessages)
	}
	if received.Load() != totalMessages {
		t.Errorf("received %d messages instead of %d (lost %d)", received.Load(), totalMessages, totalMessages-received.Load())
	}
}

// Multiple queues isolation: messages in one queue don't affect another.
func TestQueueIsolation(t *testing.T) {
	resetQueues()

	// Put to queue A
	putA := httptest.NewRequest("PUT", "/A?v=alpha", nil)
	rec := httptest.NewRecorder()
	handler(rec, putA)

	// Put to queue B
	putB := httptest.NewRequest("PUT", "/B?v=beta", nil)
	rec = httptest.NewRecorder()
	handler(rec, putB)

	// Get from A → should get alpha
	getA := httptest.NewRequest("GET", "/A", nil)
	rec = httptest.NewRecorder()
	handler(rec, getA)
	if rec.Code != 200 || rec.Body.String() != "alpha" {
		t.Errorf("A: expected 'alpha', got %q", rec.Body.String())
	}

	// Get from B → should get beta
	getB := httptest.NewRequest("GET", "/B", nil)
	rec = httptest.NewRecorder()
	handler(rec, getB)
	if rec.Code != 200 || rec.Body.String() != "beta" {
		t.Errorf("B: expected 'beta', got %q", rec.Body.String())
	}

	// A is now empty → 404
	getA2 := httptest.NewRequest("GET", "/A", nil)
	rec = httptest.NewRecorder()
	handler(rec, getA2)
	if rec.Code != 404 {
		t.Errorf("A should be empty")
	}
}

// Timeout parameter parsing: invalid timeout → 400
func TestInvalidTimeout(t *testing.T) {
	resetQueues()
	req := httptest.NewRequest("GET", "/q?timeout=abc", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != 400 {
		t.Errorf("expected 400 for non-numeric timeout, got %d", rec.Code)
	}

	req2 := httptest.NewRequest("GET", "/q?timeout=-1", nil)
	rec2 := httptest.NewRecorder()
	handler(rec2, req2)
	if rec2.Code != 400 {
		t.Errorf("expected 400 for negative timeout, got %d", rec2.Code)
	}
}

// When multiple waiters time out, they should not deadlock and all should get 404.
func TestMultipleTimeoutExpiry(t *testing.T) {
	resetQueues()
	var wg sync.WaitGroup
	count := 10
	errCh := make(chan error, count)

	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("GET", "/q?timeout=1", nil)
			rec := httptest.NewRecorder()
			start := time.Now()
			handler(rec, req)
			elapsed := time.Since(start)
			if rec.Code != 404 {
				errCh <- fmt.Errorf("expected 404 got %d", rec.Code)
			}
			if elapsed < 900*time.Millisecond {
				errCh <- fmt.Errorf("returned too early: %v", elapsed)
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Error(e)
	}
}

// Waiters that time out should be removed from the waitlist so they don't
// receive messages after they've returned 404.
func TestTimedOutWaiterDoesNotReceiveLater(t *testing.T) {
	resetQueues()

	// Start a waiter with very short timeout.
	req1 := httptest.NewRequest("GET", "/q?timeout=1", nil)
	rec1 := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler(rec1, req1)
		close(done)
	}()

	// Wait for it to time out (should be ~1s).
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter didn't finish")
	}

	if rec1.Code != 404 {
		t.Fatalf("waiter should have timed out with 404, got %d", rec1.Code)
	}

	// Now enqueue a message.
	putReq := httptest.NewRequest("PUT", "/q?v=lonely", nil)
	putRec := httptest.NewRecorder()
	handler(putRec, putReq)

	// The message should still be available for future GETs, not taken by the timed-out waiter.
	getReq := httptest.NewRequest("GET", "/q", nil)
	getRec := httptest.NewRecorder()
	handler(getRec, getReq)
	if getRec.Code != 200 || getRec.Body.String() != "lonely" {
		t.Errorf("expected to receive 'lonely', got %d %q", getRec.Code, getRec.Body.String())
	}
}
