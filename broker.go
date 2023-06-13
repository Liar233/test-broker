package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

var queues sync.Map

func main() {

	if len(os.Args) != 2 {

		log.Fatal("invalid arguments")
	}

	port, err := strconv.ParseUint(os.Args[1], 10, 64)

	if err != nil {

		log.Fatal("invalid port value")
	}

	addr := fmt.Sprintf("0.0.0.0:%d", port)

	mux := http.NewServeMux()

	mux.HandleFunc("/", handleHttp)

	if err = http.ListenAndServe(addr, mux); err != nil {

		log.Fatalf("server exit with: %s", err.Error())
	}
}

func handleHttp(w http.ResponseWriter, r *http.Request) {

	queueName := r.URL.Path

	emptyQueue := NewTopic()
	queue, _ := queues.LoadOrStore(queueName, emptyQueue)

	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Has("timeout") {

			t := r.URL.Query().Get("timeout")
			timeout, err := strconv.ParseUint(t, 10, 64)

			if err != nil {
				http.Error(w, "", http.StatusBadRequest)
				return
			}

			ctx, cancel := context.WithTimeout(r.Context(), time.Second*time.Duration(timeout))

			defer cancel()

			msg, err := queue.(*Topic).Get(ctx)

			if err == nil {
				select {
				default:
				case <-ctx.Done():
					http.Error(w, "", http.StatusBadRequest)
					return
				}
			} else {
				http.Error(w, "", http.StatusNotFound)
				return
			}

			w.WriteHeader(200)
			_, _ = w.Write([]byte(msg))
			return
		}

		msg, err := queue.(*Topic).Get(nil)

		if err != nil {
			http.Error(w, "", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(msg))
		return

	case http.MethodPut:
		msg := r.URL.Query().Get("v")
		if msg == "" {
			http.Error(w, "", http.StatusBadRequest)
			return
		}

		queue.(*Topic).Set(msg)

		w.WriteHeader(http.StatusOK)
		return

	default:
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}
}

type Topic struct {
	messages  []string
	observers []chan string
	sync.Mutex
}

func (t *Topic) Get(ctx context.Context) (string, error) {
	t.Lock()

	if len(t.messages) > 0 {

		msg := t.messages[0]

		t.messages = t.messages[1:]
		t.Unlock()

		return msg, nil
	}

	if ctx == nil {
		return "", errors.New("not found")
	}

	req := make(chan string)

	t.observers = append(t.observers, req)
	t.Unlock()

	for {
		select {
		case msg := <-req:
			return msg, nil
		case <-ctx.Done():
			req = nil
			return "", ctx.Err()
		}
	}
}

func (t *Topic) Set(msg string) {
	t.Lock()
	defer t.Unlock()

	if len(t.observers) > 0 {
		for idx, observer := range t.observers {
			if observer != nil {
				observer <- msg
				t.observers = t.observers[idx+1:]

				return
			}
		}
	}

	t.messages = append(t.messages, msg)
}

func NewTopic() *Topic {

	return &Topic{
		messages:  make([]string, 0),
		observers: make([]chan string, 0),
	}
}
