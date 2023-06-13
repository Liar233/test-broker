package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTopicSync(t *testing.T) {

	queue := NewTopic()

	queue.Set("msg1")

	if msg, err := queue.Get(context.Background()); err != nil || msg != "msg1" {

		t.Fatal("failed sync get message")
	}

	if len(queue.messages) != 0 {

		t.Fatal("failed messages count")
	}

	if len(queue.observers) != 0 {

		t.Fatal("failed observers count")
	}
}

func TestTopicAsync(t *testing.T) {

	queue := NewTopic()

	var wg sync.WaitGroup

	results := make([]string, 5)

	for i := 0; i < 5; i++ {

		wg.Add(1)

		go func(numb int) {

			ctx, cancel := context.WithCancel(context.Background())

			defer cancel()

			msg, err := queue.Get(ctx)

			if err == nil {
				results[numb] = msg
			}

			wg.Done()
		}(i)

		time.After(time.Millisecond * time.Duration(2))
	}

	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("msg%d", i)

		queue.Set(msg)
	}

	wg.Wait()

	empty := 0

	for _, res := range results {
		if res == "" {
			empty += 1
		}
	}

	if empty > 0 {

		t.Fatal("invalid messages count")
	}
}

func TestConcurrencyWriting(t *testing.T) {
	queue := NewTopic()

	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)

		go func() {

			defer wg.Done()

			timeout := time.Second * time.Duration(5)

			ctx, cancel := context.WithTimeout(context.Background(), timeout)

			defer cancel()

			msg, err := queue.Get(ctx)

			if err != nil {

				println("Not found")
				return
			}

			if err == nil {
				select {
				default:
				case <-ctx.Done():
					println("Timeout")
					return
				}
			}

			println(msg)
		}()
	}

	<-time.After(time.Second)

	println("Add msg")
	queue.Set("msg1")
	println("Msg added")

	wg.Wait()
}
