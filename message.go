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
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
)

const (
	// MessageDuplicationID is the duplication hash
	MessageDuplicationID = "DuplicationID"
	// MessageID is the id of the message
	MessageID = "MessageID"
	// MessageGroupID is the param for the group id
	MessageGroupID = "MessageGroupID"
	// MessageReceipt is the receipt id
	MessageReceipt = "ReceiptHandle"
	// MessageMD5 is the md5 of the message
	MessageMD5 = "MessageMD5"
)

// messageImpl is the queue message
type messageImpl struct {
	// context is the context for the message
	context context.Context
	// body is the message body
	body *bytes.Buffer
	// attributes of the message
	attributes map[string]string
}

// newMessage creates, initializes and returns an empty message
func newMessage() *messageImpl {
	return &messageImpl{
		attributes: make(map[string]string, 0),
		body:       new(bytes.Buffer),
		context:    context.Background(),
	}
}

// Checksum returns the message body checksum
func (m *messageImpl) Checksum() string {
	h := sha256.New()
	h.Write(m.body.Bytes())

	return fmt.Sprintf("%x", h.Sum(nil))
}

// Context returns the message context
func (m *messageImpl) Context() context.Context {
	return m.context
}

// BodyInString returns the body in string
func (m *messageImpl) BodyInString() string {
	return m.body.String()
}

// Attributes returns the attributes
func (m *messageImpl) Attributes() map[string]string {
	return m.attributes
}

// SetAttribute sets an attribute on the message
func (m *messageImpl) SetAttribute(k, v string) {
	m.attributes[k] = v
}

// GetAttribute returns a attribute
func (m *messageImpl) GetAttribute(k string) string {
	return m.attributes[k]
}

// MessageGroupID sets the message group id
func (m *messageImpl) MessageGroupID(v string) {
	m.attributes[MessageGroupID] = v
}

// Body returns a reader to the body
func (m *messageImpl) Body() io.Reader {
	return bytes.NewReader(m.body.Bytes())
}

// Write writes the message into the buffer
func (m *messageImpl) Write(b []byte) (int, error) {
	return m.body.Write(b)
}

// String returns a representation of the message
func (m *messageImpl) String() string {
	return fmt.Sprintf("id: %s, body: %s",
		m.attributes[MessageGroupID],
		m.body.String())
}
