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
	"errors"
)

// topicWriter implements the topic writer interface
type topicWriter struct {
	*messageImpl
	// client is the topic client
	client *topicImpl
	// closed indicate the message has been sent
	closed bool
}

// Close is responsible for closing and sending the message
func (t *topicWriter) Close() error {
	return t.Send()
}

// Send is resposible for sending the message on the queue
func (t *topicWriter) Send() error {
	if t.closed {
		return errors.New("writer is closed")
	}
	err := t.client.sendMessage(t)
	if err == nil {
		t.closed = true
	}

	return err
}
