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
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/jpillora/backoff"
)

// newSQSClient returns an sqs client from the configuration
func newSQSClient(options *Options) sqsiface.SQSAPI {
	cfg := &aws.Config{}
	if options.Region != "" {
		cfg.Region = aws.String(options.Region)
	}
	if options.AccessID != "" && options.AccessKey != "" {
		cfg.Credentials = credentials.NewStaticCredentials(options.AccessID, options.AccessKey, "")
	}
	if options.Profile != "" {
		filename := fmt.Sprintf("%s/.aws/credentials", os.Getenv("HOME"))
		cfg.Credentials = credentials.NewSharedCredentials(filename, options.Profile)
	}
	if options.Retries > 0 {
		cfg.MaxRetries = aws.Int(options.Retries)
	}
	if options.Debug {
		cfg.LogLevel = aws.LogLevel(aws.LogDebugWithHTTPBody)
		cfg.Logger = &loggerImpl{logger: options.Logger}
		cfg.Logger = aws.NewDefaultLogger()
	}

	return sqs.New(session.Must(session.NewSession()), cfg)
}

// convertMessageAttributesToAttributes converts the sqs attributes to message attributes
func convertMessageAttributesToAttributes(attrs map[string]*sqs.MessageAttributeValue) map[string]string {
	attributes := make(map[string]string, 0)

	for k, v := range attrs {
		switch aws.StringValue(v.DataType) {
		case "String":
			attributes[k] = aws.StringValue(v.StringValue)
		}
	}

	return attributes
}

// convertAttributesToMessageAttribute converts the message attributes to aws ones
func convertAttributesToMessageAttribute(attrs map[string]string) map[string]*sqs.MessageAttributeValue {
	attributes := make(map[string]*sqs.MessageAttributeValue, 0)
	for k, v := range attrs {
		attributes[k] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}

	return attributes
}

// retry attempts to perform an operation for x time
func retry(attempts int, min time.Duration, fn func() error) error {
	var err error

	b := &backoff.Backoff{Factor: 2, Jitter: true, Min: min}
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		time.Sleep(b.Duration())
	}

	return err
}
