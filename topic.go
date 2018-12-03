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
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type topicImpl struct {
	// the configuration for the topic writer
	config *Options
	// client is the sqs client
	client sqsiface.SQSAPI
}

// NewTopic creates and returns a new topic writer
func NewTopic(options ...Option) (Topic, error) {
	config := &Options{}
	config.Handle(options...)

	if config.Name == "" {
		return nil, errors.New("no topic name defined")
	}

	return &topicImpl{client: newSQSClient(config), config: config}, nil
}

// Name returns the name of the topic
func (t *topicImpl) Name() string {
	return t.config.Name
}

// Writer creates and returns a topic writer for sending a message
func (t *topicImpl) Writer() TopicWriter {
	return &topicWriter{client: t, messageImpl: newMessage()}
}

// sendMessage is responsible for sending the message on the queue
func (t *topicImpl) sendMessage(m Message) error {
	// @step: construct the message input
	input := &sqs.SendMessageInput{
		MessageAttributes: convertAttributesToMessageAttribute(m.Attributes()),
		MessageBody:       aws.String(m.BodyInString()),
		QueueUrl:          aws.String(t.config.Name),
	}
	if t.config.DuplicationID {
		input.MessageDeduplicationId = aws.String(m.Checksum())
	}
	if v, found := m.Attributes()[MessageGroupID]; found {
		input.MessageGroupId = aws.String(v)
	}

	return retry(t.config.Retries, 2*time.Second, func() error {
		_, err := t.client.SendMessage(input)

		return err
	})
}
