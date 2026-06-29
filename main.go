package main

import (
	"flag"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type waiter struct {
	ticket uint64
	ch     chan string
}

type MessageQueue struct {
	mu         sync.Mutex
	messages   []string
	waiters    []*waiter
	nextTicket atomic.Uint64
}

func (q *MessageQueue) Put(msg string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.waiters) > 0 {
		// Deliver to the earliest waiter (smallest ticket).
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		w.ch <- msg
		return
	}
	q.messages = append(q.messages, msg)
}

func (q *MessageQueue) Get(timeout int) (string, bool) {
	ticket := q.nextTicket.Add(1) // arrival order

	q.mu.Lock()
	// Check queue first.
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		q.mu.Unlock()
		return msg, true
	}

	if timeout <= 0 {
		q.mu.Unlock()
		return "", false
	}

	ch := make(chan string, 1)
	w := &waiter{ticket: ticket, ch: ch}
	// Insert into waiters sorted by ticket.
	idx := sort.Search(len(q.waiters), func(i int) bool {
		return q.waiters[i].ticket > ticket
	})
	q.waiters = append(q.waiters, nil)
	copy(q.waiters[idx+1:], q.waiters[idx:])
	q.waiters[idx] = w

	// Timeout goroutine.
	go func() {
		time.Sleep(time.Duration(timeout) * time.Second)
		q.mu.Lock()
		for i, waiter := range q.waiters {
			if waiter == w {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				close(ch)
				break
			}
		}
		q.mu.Unlock()
	}()

	q.mu.Unlock()

	msg, ok := <-ch
	if !ok {
		return "", false
	}
	return msg, true
}

var (
	queuesMu sync.Mutex
	queues   = map[string]*MessageQueue{}
)

func getQueue(name string) *MessageQueue {
	queuesMu.Lock()
	defer queuesMu.Unlock()
	if q, ok := queues[name]; ok {
		return q
	}
	q := &MessageQueue{}
	queues[name] = q
	return q
}

func handler(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Path
	if len(queue) > 0 && queue[0] == '/' {
		queue = queue[1:]
	}
	if queue == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		v := r.URL.Query()["v"]
		if len(v) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		getQueue(queue).Put(v[0])
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		timeout := 0
		if tStr := r.URL.Query().Get("timeout"); tStr != "" {
			t, err := strconv.Atoi(tStr)
			if err != nil || t < 0 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			timeout = t
		}
		msg, ok := getQueue(queue).Get(timeout)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(msg))

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func main() {
	port := flag.Int("port", 8080, "server port")
	flag.Parse()

	http.HandleFunc("/", handler)
	http.ListenAndServe(":"+strconv.Itoa(*port), nil)
}
