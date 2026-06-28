# queue-broker

A simple in-memory queue broker written in Go, exposing an HTTP API with two endpoints.

## Features

- In-memory FIFO queues
- Thread-safe, concurrent access
- Timeout support for blocking GET requests
- Configurable port
- Zero external dependencies (standard library only)

## API

### PUT `/{queue}?v=message`

Enqueues a message into the specified queue. Returns 200 OK on success, 400 Bad Request if the `v` parameter is missing.

Example:
    curl -XPUT http://localhost:8080/pet?v=cat

### GET `/{queue}`

Dequeues a message from the queue in FIFO order. Returns the message body with 200 OK, or 404 Not Found if the queue is empty.

Optional `timeout` parameter: `GET /{queue}?timeout=N`. If no message is available, the request blocks for up to `N` seconds until a message arrives or the timeout expires. Returns 404 if timed out.

Example:
    curl http://localhost:8080/pet
    curl http://localhost:8080/pet?timeout=5

## Building and running

    go build -o queue-broker
    ./queue-broker -port=8080

The default port is 8080.

## Testing

Use curl commands as shown. Multiple clients can wait for messages concurrently; the first queued waiter receives the first available message.

## Assignment context

This project was implemented as a test assignment for a Go developer position. It meets the specified requirements: only standard library, single file, concise code.
