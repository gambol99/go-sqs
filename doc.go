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

// Package queue contains the code for handling queues
package queue

/*
	broker, err := queue.New(
		queue.WithRegion("eu-west-2"),
		queue.WithQueue("name"),
		queue.WithProfile("ecr-test")
	})
	if err != nil {
		return err
	}

	// @step: create a topic writer to a queue
	topic, err := queue.Topic("name")
	if err != nil {
		return err
	}

	// @step: receive messages
	err := broker.Serve(&queue.HandlerFunc{
		OnMessage: func(message queue.Message) error {


		},
	})
	if err != nil {
		return err
	}

	// @step: send a message a queue
	w := topic.Writer()
	w.Attributes("id", "key")
	w.Write([]byte("some body"))

	// and to send either Close()
	if err := w.Close(); err != nil {
		return err
	}
	// or
	if err := w.Send(); err != nil {
		return err
	}
*/
