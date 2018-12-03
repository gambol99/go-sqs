/*
Copyright 2018 Rohith Jayawardene <gambol99@gmail.com> All rights reserved.

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

package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/jpillora/backoff"
)

// qqueueImpl implements the queue
type queueImpl struct {
	// config is the configuration for the service
	config *Options
	// client is the sqs client
	client sqsiface.SQSAPI
	// messageCh is the channel for workers to receive messages
	messageCh chan *messageImpl
	// stopCh is a stop channel
	stopCh chan struct{}
	// handlers is the receive handler
	handlers Handlers
	// wg is a waitgroup for tasks
	wg *sync.WaitGroup
}

// New creates and returns a queue client
func New(options ...Option) (Queue, error) {
	config := &Options{}
	config.Handle(options...)

	if config.Name == "" {
		return nil, errors.New("no queue name defined")
	}
	return &queueImpl{
		client:    newSQSClient(config),
		config:    config,
		stopCh:    make(chan struct{}, 0),
		messageCh: make(chan *messageImpl, config.Concurrency),
		wg:        new(sync.WaitGroup),
	}, nil
}

// Name returns the name of the queue
func (q *queueImpl) Name() string {
	return q.config.Name
}

// log is responsible for logging a message
func (q *queueImpl) log(s string, args ...interface{}) {
	q.config.Logger(s, args...)
}

// Serve adds the event listener and start recieving messages off the queue
func (q *queueImpl) Serve(h Handlers) error {
	var ctx context.Context

	if h == nil {
		return errors.New("no handlers defined")
	}
	q.handlers = h

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// @step: create a controller routine
	go func() {
		<-q.stopCh
		cancel()
	}()

	bo := &backoff.Backoff{Factor: 2, Jitter: true, Min: 500 * time.Millisecond}

	// @step: create the workers to handle the message
	for i := 0; i < q.config.Concurrency; i++ {
		go q.processNextItem()
	}

	// @step: create a rate limit if required
	var throttle <-chan time.Time
	if q.config.RateLimit > 0 {
		throttle = time.Tick(time.Second / time.Duration(q.config.RateLimit))
	}

	q.log("entering into the service loop for task queue: %s", q.config.Name)

	// @step: create the service loop and wait for messages within an anonymous function
	func() {
		for {
			select {
			case <-ctx.Done():
				q.log("received a signal to terminate the queue, shutting down gracefully")
				return
			default:
				err := func() error {
					resp, err := q.client.ReceiveMessageWithContext(ctx,
						&sqs.ReceiveMessageInput{
							MaxNumberOfMessages:   aws.Int64(q.config.MaxMessages),
							MessageAttributeNames: aws.StringSlice(q.config.MessageAttributesFilter),
							QueueUrl:              aws.String(q.config.Name),
							VisibilityTimeout:     aws.Int64(int64(q.config.VisibilityTimeout.Seconds())),
							WaitTimeSeconds:       aws.Int64(20),
						},
					)
					if err != nil {
						if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
							return nil
						}

						tm := bo.Duration()
						q.log("received an error: %s from sqs, backing off for: %s", err.Error(), tm.String())
						time.Sleep(tm)

						return err
					}
					bo.Reset()
					q.log("received %d message from the queue", len(resp.Messages))

					// @step: push the message into the worker channel
					for _, x := range resp.Messages {
						m := newMessage()
						m.attributes = convertMessageAttributesToAttributes(x.MessageAttributes)
						m.attributes[MessageID] = aws.StringValue(x.MessageId)
						m.attributes[MessageReceipt] = aws.StringValue(x.ReceiptHandle)
						m.attributes[MessageMD5] = aws.StringValue(x.MD5OfBody)
						m.body.WriteString(aws.StringValue(x.Body))
						m.context = context.WithValue(ctx, MessageID, aws.StringValue(x.MessageId))

						// @step: if we are rate limiting grab a token to operate
						if q.config.RateLimit > 0 {
							<-throttle
						}

						// @step: send to a worker to process the request
						q.messageCh <- m
					}

					return nil
				}()
				if err != nil {
					go q.handlers.ErrorHandler(err)
				}
			}
		}
	}()

	q.log("waiting for any running and scheduled tasks to complete / cancel before terminating")

	// @logic: we wait for any running or received tasks to finish and then we close the
	// workers channel - we then send a signal to indicate we've finished
	q.wg.Wait()
	// close off and terminate the worker
	close(q.messageCh)
	// signal the stop its closed
	q.stopCh <- struct{}{}

	return nil
}

// processNextItem is the worker routine used to handle the messages
func (q *queueImpl) processNextItem() {
	for x := range q.messageCh {
		q.log("worker is handling the message: %s", x.GetAttribute(MessageID))

		err := func() error {
			q.wg.Add(1)
			// remove the task off the list
			defer q.wg.Done()

			// @step: call the handler
			if err := q.handlers.ReceiveHandler(x); err != nil {
				q.log("handler failed to process message: %s, error: %s", x.GetAttribute(MessageID), err)

				return err
			}

			return retry(q.config.Retries, 500*time.Millisecond, func() error {
				q.log("attempting to delete the message: %s", x.GetAttribute(MessageID))

				_, err := q.client.DeleteMessageWithContext(x.context,
					&sqs.DeleteMessageInput{
						QueueUrl:      aws.String(q.config.Name),
						ReceiptHandle: aws.String(x.GetAttribute(MessageReceipt)),
					})

				return err
			})
		}()
		if err != nil {
			go q.handlers.ErrorHandler(err)
		}
	}
}

// Stop is used to stop the receiver
func (q *queueImpl) Stop() error {
	// signal the shutdown of the queue
	q.stopCh <- struct{}{}
	// wait for the queue to indicates its shutdown gracefully
	<-q.stopCh

	return nil
}
