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

package queue

import (
	"context"
	"io"
	"time"
)

// Logger is the interface for the logger
type Logger func(string, ...interface{})

// Options is the options for the queue
type Options struct {
	// AccessID is the aws access id to use
	AccessID string
	// AccessKey is the aws access key to use
	AccessKey string
	// Concurrency is the number of messages to send concurrently
	Concurrency int
	// Debug indicates if we want debugging
	Debug bool
	// DeplicationID indicates we should generate a duplicate id
	DuplicationID bool
	// MaxMessages is the max message to return
	MaxMessages int64
	// MessageAttributesFilter is a collection of attributes to filter on
	MessageAttributesFilter []string
	// Name is the name of the queue
	Name string
	// Logger is a logging method
	Logger Logger
	// Profile is the aws credentials profile to use
	Profile string
	// RateLimit is a limit on the messages per second
	RateLimit int64
	// Region is the aws region
	Region string
	// Retries is the number of retries
	Retries int
	// VisibilityTimeout is the time a message remain hidden post receiving
	VisibilityTimeout time.Duration
}

// Option defines an option method
type Option func(*Options)

// Message is the contract for a message
type Message interface {
	io.Writer
	// Attributes returns the message attributes
	Attributes() map[string]string
	// Body reads the message body io.reader
	Body() io.Reader
	// BodyInString returns the message body in string
	BodyInString() string
	// Checksum returns the checksum of the content
	Checksum() string
	// Context returns the message context
	Context() context.Context
	// GetAttribute gets a message attributes
	GetAttribute(string) string
	// MessageGroupID sets the message group id
	MessageGroupID(string)
	// SetAttribute sets a message attribute
	SetAttribute(string, string)
}

// Topic is the interface for a topic
type Topic interface {
	// Name is the name of the topic / queue
	Name() string
	// Writer hands back a writer to the topic
	Writer() TopicWriter
}

// TopicWriter is contract for a message writer
type TopicWriter interface {
	// implement the message interface
	Message
	// on Close the message writer will automatically attempt to write the message
	io.Closer
	// Send sends the response to the topic
	Send() error
}

// Handlers is the queue handler
type Handlers interface {
	// ReceiveHandler is response for recieving a message from the queue
	ReceiveHandler(Message) error
	// ErrorHandler is called on a queue error
	ErrorHandler(error)
}

// Queue is the contract to the sqs queue
type Queue interface {
	// Serve adds and starts event loop
	Serve(Handlers) error
	// Name returns the name of the queue
	Name() string
	// Stop is called to stop the queue gracefully
	Stop() error
}

// HandlerFuncs returns a implementation for the reciever
type HandlerFuncs struct {
	// OnMessageFunc is the receiver func
	OnMessageFunc func(Message) error
	// OnErrorFunc is called on an error
	OnErrorFunc func(error)
}

// ReceiveHandler is response for recieving a message from the queue
func (h *HandlerFuncs) ReceiveHandler(m Message) error {
	if h.OnMessageFunc != nil {
		return h.OnMessageFunc(m)
	}

	return nil
}

// ErrorHandler is called on a queue error
func (h *HandlerFuncs) ErrorHandler(err error) {
	if h.OnErrorFunc != nil {
		h.OnErrorFunc(err)
	}
}

type loggerImpl struct {
	logger Logger
}

func (l *loggerImpl) Log(o ...interface{}) {
	if len(o) <= 0 {
		return
	}

	l.logger(o[0].(string), o[1:])
}
