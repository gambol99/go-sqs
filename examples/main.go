/*
Copyright 2018 Rohith Jayawardene All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gambol99/go-sqs"
)

func main() {
	logger := func(s string, args ...interface{}) {
		fmt.Printf("[log] "+s, args...)
	}

	// @step: create the service broker
	options := []queue.Option{
		queue.WithAccessID(os.Getenv("AWS_ACCESS_KEY_ID")),
		queue.WithAccessKey(os.Getenv("AWS_SECRET_ACCESS_KEY")),
		queue.WithConcurrency(10),
		queue.WithDebug(false),
		queue.WithDuplicationID(false),
		queue.WithLogger(logger),
		queue.WithMaxMessages(10),
		queue.WithName(os.Getenv("TASK_QUEUE")),
		queue.WithProfile(os.Getenv("AWS_PROFILE")),
		queue.WithRegion(os.Getenv("AWS_REGION")),
		queue.WithRetries(3),
		queue.WithVisibilityTimeout(5 * time.Minute),
	}

	// create the queue broker
	broker, err := queue.New(options...)
	if err != nil {
		return
	}

	// @step: create a writer back to a queue
	options = append(options, queue.WithName(os.Getenv("REPLY_QUEUE")))
	writer, err := queue.NewTopic(options...)
	if err != nil {
		return
	}

	// @step: start receiving messages from the queue
	errCh := make(chan error, 0)
	go func() {
		err := broker.Serve(&queue.HandlerFuncs{
			OnMessageFunc: func(m queue.Message) error {
				fmt.Printf("recieved a message: %v\n", m)

				wr := writer.Writer()
				wr.Write([]byte("hello world"))
				wr.SetAttribute("SomeAttribute", "Hello")

				// we can send the message by closing the message or calling send()
				if err := wr.Send(); err != nil {
					fmt.Fprintf(os.Stderr, "[error] %s\n", err)
				}

				// @note: if the handler returns a nil error we delete the message from the queue

				return nil
			},
			OnErrorFunc: func(e error) {
				fmt.Printf("[error] %s\n", e)
			},
		})
		if err != nil {
			errCh <- err
		}
	}()

	signalChannel := make(chan os.Signal)
	signal.Notify(signalChannel, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case <-signalChannel:
	case e := <-errCh:
		fmt.Fprintf(os.Stderr, "[error] %s\n", e)
		os.Exit(1)
	}

	if err := broker.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "[error] failed to shut down broker gracefully: %s\n", err)
		os.Exit(1)
	}
}
