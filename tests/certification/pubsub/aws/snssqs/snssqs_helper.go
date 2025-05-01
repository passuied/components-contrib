/*
Copyright 2022 The Dapr Authors
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

package snssqs_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

var (
	partition   string = "aws"
	serviceName string = "sns"
)

func deleteQueues(ctx context.Context, queues []string) error {
	svc := sqsService(ctx)
	for _, queue := range queues {
		if err := deleteQueue(ctx, svc, queue); err != nil {
			fmt.Printf("error deleting the queue URL: %q err:%v", queue, err)
		}
	}
	return nil
}

func deleteQueue(ctx context.Context, svc *sqs.Client, queue string) error {
	fmt.Printf("deleteQueue: %q\n", queue)
	queueUrl, err := getQueueURL(ctx, svc, queue)
	if err != nil {
		return fmt.Errorf("error getting the queue URL: %q err:%v", queue, err)
	}

	_, err = svc.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: &queueUrl,
	})

	return err
}

func getQueueURL(ctx context.Context, svc *sqs.Client, queue string) (string, error) {
	urlResult, err := svc.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queue),
	})

	if err != nil {
		return "", err
	}

	return *urlResult.QueueUrl, nil
}

func getMessages(ctx context.Context, svc *sqs.Client, queueURL string) (*sqs.ReceiveMessageOutput, error) {
	input := &sqs.ReceiveMessageInput{
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateReceiveCount,
		},
		MaxNumberOfMessages: 10,
		QueueUrl:            &queueURL,
		VisibilityTimeout:   5,
		WaitTimeSeconds:     20,
	}

	msgResult, err := svc.ReceiveMessage(ctx, input)
	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

func deleteMessage(ctx context.Context, svc *sqs.Client, queueURL, messageHandle string) error {
	_, err := svc.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: &messageHandle,
	})
	return err
}

func deleteTopics(ctx context.Context, topics []string, region string) error {
	svc := snsService(ctx, region)
	id, err := getIdentity(ctx, region)
	if err != nil {
		return err
	}

	for _, topic := range topics {
		topicArn := buildARN(partition, serviceName, topic, region, id)
		fmt.Printf("Getting subscriptions for topicArn: %s\n", topicArn)
		if subout, err := svc.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
			TopicArn: aws.String(topicArn),
		}); err == nil {
			for _, sub := range subout.Subscriptions {
				if err := unsubscribeFromTopic(ctx, svc, *sub.SubscriptionArn); err != nil {
					fmt.Printf("error unsubscribing arn: %q err:%v\n", *sub.SubscriptionArn, err)
				}
			}
		} else {
			fmt.Printf("error getting subscription list topic: %q err:%v\n", topic, err)
		}

		if err := deleteTopic(ctx, svc, topicArn); err != nil {
			fmt.Printf("error deleting the topic: %q err:%v\n", topic, err)
		}
	}
	return nil
}

func deleteTopic(ctx context.Context, svc *sns.Client, topic string) error {
	fmt.Printf("deleteTopic: %q\n", topic)
	_, err := svc.DeleteTopic(ctx, &sns.DeleteTopicInput{
		TopicArn: aws.String(topic),
	})

	return err
}

func unsubscribeFromTopic(ctx context.Context, svc *sns.Client, subscription string) error {
	_, err := svc.Unsubscribe(ctx, &sns.UnsubscribeInput{
		SubscriptionArn: aws.String(subscription),
	})

	return err
}

func sqsService(ctx context.Context) *sqs.Client {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	return sqs.NewFromConfig(cfg)
}

func snsService(ctx context.Context, region string) *sns.Client {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		panic(err)
	}
	return sns.NewFromConfig(cfg)
}

func getIdentity(ctx context.Context, region string) (*sts.GetCallerIdentityOutput, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	svc := sts.NewFromConfig(cfg)
	input := &sts.GetCallerIdentityInput{}
	result, err := svc.GetCallerIdentity(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if ok := errorAs(err, &apiErr); ok {
			return nil, fmt.Errorf(apiErr.ErrorMessage())
		}
		return nil, err
	}
	return result, nil
}

func buildARN(partition, serviceName, entityName, region string, id *sts.GetCallerIdentityOutput) string {
	return fmt.Sprintf("arn:%s:%s:%s:%s:%s", partition, serviceName, region, *id.Account, entityName)
}

type QueueManager struct {
	svc *sqs.Client
}

type SNSMessagePayload struct {
	Message  string
	TopicArn string
}
type DataMessage struct {
	Data string `json:"data"`
}

type MessageFunc func(*DataMessage) error

func NewQueueManager(ctx context.Context) *QueueManager {
	qm := QueueManager{}
	qm.connect(ctx)
	return &qm
}

func (qm *QueueManager) connect(ctx context.Context) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	qm.svc = sqs.NewFromConfig(cfg)
	return nil
}

func (qm *QueueManager) GetMessages(ctx context.Context, queue string, deleteMsg bool, mf MessageFunc) (int, error) {
	queueURL, err := getQueueURL(ctx, qm.svc, queue)
	if err != nil {
		return -1, err
	}

	msgResult, err := getMessages(ctx, qm.svc, queueURL)
	if err != nil {
		return -1, err
	}

	numMgs := len(msgResult.Messages)
	for _, msg := range msgResult.Messages {
		dm, err := extractDataMessage(msg)
		if err != nil {
			return -1, err
		}

		if err := mf(dm); err != nil {
			return -1, err
		}
		if deleteMsg {
			err = deleteMessage(ctx, qm.svc, queueURL, *msg.ReceiptHandle)
			if err != nil {
				return -1, err
			}
		}
	}

	return numMgs, nil
}

func extractDataMessage(msg sqstypes.Message) (*DataMessage, error) {
	snsMP := SNSMessagePayload{}
	err := json.Unmarshal([]byte(*(msg.Body)), &snsMP)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling message Body: %v", err)
	}
	dm := DataMessage{}
	err = json.Unmarshal([]byte(snsMP.Message), &dm)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling message data: %v", err)
	}

	return &dm, nil
}
