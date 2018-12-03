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

import "time"

// Handle handles the options
func (o *Options) Handle(opts ...Option) {
	for _, fn := range opts {
		fn(o)
	}
	o.Defaults()
}

// Defaults sets some defaults
func (o *Options) Defaults() {
	if o.Concurrency <= 0 {
		o.Concurrency = 1
	}
	if o.MaxMessages <= 0 {
		o.MaxMessages = 1
	}
	if len(o.MessageAttributesFilter) == 0 {
		o.MessageAttributesFilter = append(o.MessageAttributesFilter, "All")
	}
	if o.Logger == nil {
		o.Logger = func(s string, args ...interface{}) {}
	}
	if o.Retries <= 0 {
		o.Retries = 3
	}
	if o.VisibilityTimeout <= 0 {
		o.VisibilityTimeout = 5 * time.Minute
	}
}

// WithLogger sets the logger
func WithLogger(v Logger) Option {
	return func(o *Options) {
		o.Logger = v
	}
}

// WithDebug sets the debug option
func WithDebug(v bool) Option {
	return func(o *Options) {
		o.Debug = v
	}
}

// WithRateLimit sets a rate limit
func WithRateLimit(v int64) Option {
	return func(o *Options) {
		o.RateLimit = v
	}
}

// WithConcurrency sets the worker concurrency
func WithConcurrency(v int) Option {
	return func(o *Options) {
		o.Concurrency = v
	}
}

// WithMessageAttributeFilter adds a filter on a queue
func WithMessageAttributeFilter(v string) Option {
	return func(o *Options) {
		o.MessageAttributesFilter = append(o.MessageAttributesFilter, v)
	}
}

// WithVisibilityTimeout sets the queue visibility timeout
func WithVisibilityTimeout(v time.Duration) Option {
	return func(o *Options) {
		o.VisibilityTimeout = v
	}
}

// WithName is the queue to read from
func WithName(v string) Option {
	return func(o *Options) {
		o.Name = v
	}
}

// WithMaxMessages sets the max messages
func WithMaxMessages(v int64) Option {
	return func(o *Options) {
		o.MaxMessages = v
	}
}

// WithRetries set the number of retries
func WithRetries(v int) Option {
	return func(o *Options) {
		o.Retries = v
	}
}

// WithAccessID is the aws access id to use
func WithAccessID(v string) Option {
	return func(o *Options) {
		o.AccessID = v
	}
}

// WithAccessKey is the aws access key to use
func WithAccessKey(v string) Option {
	return func(o *Options) {
		o.AccessKey = v
	}
}

// WithDuplicationID indicates we should generate a duplicate id
func WithDuplicationID(b bool) Option {
	return func(o *Options) {
		o.DuplicationID = b
	}
}

// WithProfile is the aws credentials profile to use
func WithProfile(v string) Option {
	return func(o *Options) {
		o.Profile = v
	}
}

// WithRegion is the aws region
func WithRegion(v string) Option {
	return func(o *Options) {
		o.Region = v
	}
}
