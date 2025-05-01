/*
Copyright 2024 The Dapr Authors
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

package aws

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	aws2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

type Clients struct {
	mu sync.RWMutex

	s3             *S3Clients
	Dynamo         *DynamoDBClients
	sns            *SnsClients
	sqs            *SqsClients
	snssqs         *SnsSqsClients
	Secret         *SecretManagerClients
	ParameterStore *ParameterStoreClients
	kinesis        *KinesisClients
	ses            *SesClients
	kafka          *KafkaClients
}

func newClients() *Clients {
	return new(Clients)
}

func (c *Clients) refresh(cfg aws2.Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.s3 != nil:
		c.s3.New(cfg)
	case c.Dynamo != nil:
		c.Dynamo.New(cfg)
	case c.sns != nil:
		c.sns.New(cfg)
	case c.sqs != nil:
		c.sqs.New(cfg)
	case c.snssqs != nil:
		c.snssqs.New(cfg)
	case c.Secret != nil:
		c.Secret.New(cfg)
	case c.ParameterStore != nil:
		c.ParameterStore.New(cfg)
	case c.kinesis != nil:
		c.kinesis.New(cfg)
	case c.ses != nil:
		c.ses.New(cfg)
	case c.kafka != nil:
		err := c.kafka.New(cfg, nil)
		if err != nil {
			return fmt.Errorf("failed to refresh Kafka AWS IAM Config: %w", err)
		}
	}
	return nil
}

type S3Clients struct {
	S3         *s3.Client
	Uploader   *manager.Uploader
	Downloader *manager.Downloader
}

type DynamoDBClients struct {
	DynamoDB *dynamodb.Client
}

type SnsSqsClients struct {
	Sns *sns.Client
	Sqs *sqs.Client
	Sts *sts.Client
}

type SnsClients struct {
	Sns *sns.Client
}

type SqsClients struct {
	Sqs *sqs.Client
}

type SecretManagerClients struct {
	Manager *secretsmanager.Client
}

type ParameterStoreClients struct {
	Store *ssm.Client
}

type KinesisClients struct {
	Kinesis *kinesis.Client
	Region  string
}

type SesClients struct {
	Ses *ses.Client
}

type KafkaClients struct {
	config          *sarama.Config
	consumerGroup   *string
	brokers         *[]string
	maxMessageBytes *int

	ConsumerGroup sarama.ConsumerGroup
	Producer      sarama.SyncProducer
}

func (c *S3Clients) New(cfg aws2.Config) {
	c.S3 = s3.NewFromConfig(cfg)
	c.Uploader = manager.NewUploader(c.S3)
	c.Downloader = manager.NewDownloader(c.S3)
}

func (c *DynamoDBClients) New(cfg aws2.Config) {
	c.DynamoDB = dynamodb.NewFromConfig(cfg)
}

func (c *SnsClients) New(cfg aws2.Config) {
	c.Sns = sns.NewFromConfig(cfg)
}

func (c *SnsSqsClients) New(cfg aws2.Config) {
	c.Sns = sns.NewFromConfig(cfg)
	c.Sqs = sqs.NewFromConfig(cfg)
	c.Sts = sts.NewFromConfig(cfg)
}

func (c *SqsClients) New(cfg aws2.Config) {
	c.Sqs = sqs.NewFromConfig(cfg)
}

func (c *SqsClients) QueueURL(ctx context.Context, queueName string) (*string, error) {
	if c.Sqs != nil {
		result, err := c.Sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
			QueueName: aws2.String(queueName),
		})
		if result != nil {
			return result.QueueUrl, err
		}
	}
	return nil, errors.New("unable to get queue url due to empty client")
}

func (c *SecretManagerClients) New(cfg aws2.Config) {
	c.Manager = secretsmanager.NewFromConfig(cfg)
}

func (c *ParameterStoreClients) New(cfg aws2.Config) {
	c.Store = ssm.NewFromConfig(cfg)
}

func (c *KinesisClients) New(cfg aws2.Config) {
	c.Kinesis = kinesis.NewFromConfig(cfg)
	c.Region = cfg.Region
}

func (c *KinesisClients) Stream(ctx context.Context, streamName string) (*string, error) {
	if c.Kinesis != nil {
		result, err := c.Kinesis.DescribeStream(ctx, &kinesis.DescribeStreamInput{
			StreamName: aws2.String(streamName),
		})
		if result != nil {
			return result.StreamDescription.StreamARN, err
		}
	}
	return nil, errors.New("unable to get stream arn due to empty client")
}

func (c *SesClients) New(cfg aws2.Config) {
	c.Ses = ses.NewFromConfig(cfg)
}

type KafkaOptions struct {
	Config          *sarama.Config
	ConsumerGroup   string
	Brokers         []string
	MaxMessageBytes int
}

func initKafkaClients(opts KafkaOptions) *KafkaClients {
	return &KafkaClients{
		config:          opts.Config,
		consumerGroup:   &opts.ConsumerGroup,
		brokers:         &opts.Brokers,
		maxMessageBytes: &opts.MaxMessageBytes,
	}
}

func (c *KafkaClients) New(cfg aws2.Config, tokenProvider *mskTokenProvider) error {
	const timeout = 10 * time.Second
	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get credentials from config: %w", err)
	}

	// fill in token provider common fields across x509 and static auth
	if tokenProvider == nil {
		tokenProvider = &mskTokenProvider{}
	}
	tokenProvider.generateTokenTimeout = timeout
	tokenProvider.region = cfg.Region
	tokenProvider.accessKey = creds.AccessKeyID
	tokenProvider.secretKey = creds.SecretAccessKey
	tokenProvider.sessionToken = creds.SessionToken

	c.config.Net.SASL.Enable = true
	c.config.Net.SASL.Mechanism = sarama.SASLTypeOAuth
	c.config.Net.SASL.TokenProvider = tokenProvider

	_, err = c.config.Net.SASL.TokenProvider.Token()
	if err != nil {
		return fmt.Errorf("error validating iam credentials %v", err)
	}

	consumerGroup, err := sarama.NewConsumerGroup(*c.brokers, *c.consumerGroup, c.config)
	if err != nil {
		return err
	}
	c.ConsumerGroup = consumerGroup

	producer, err := c.getSyncProducer()
	if err != nil {
		return err
	}
	c.Producer = producer

	return nil
}

// Kafka specific
type mskTokenProvider struct {
	generateTokenTimeout time.Duration
	accessKey            string
	secretKey            string
	sessionToken         string
	awsIamRoleArn        string
	awsStsSessionName    string
	region               string
}

func (m *mskTokenProvider) Token() (*sarama.AccessToken, error) {
	// this function can't use the context passed on Init because that context would be cancelled right after Init
	ctx, cancel := context.WithTimeout(context.Background(), m.generateTokenTimeout)
	defer cancel()

	switch {
	// we must first check if we are using the assume role auth profile
	case m.awsIamRoleArn != "" && m.awsStsSessionName != "":
		token, _, err := signer.GenerateAuthTokenFromRole(ctx, m.region, m.awsIamRoleArn, m.awsStsSessionName)
		return &sarama.AccessToken{Token: token}, err
	case m.accessKey != "" && m.secretKey != "":
		token, _, err := signer.GenerateAuthTokenFromCredentialsProvider(ctx, m.region, aws2.CredentialsProviderFunc(func(ctx context.Context) (aws2.Credentials, error) {
			return aws2.Credentials{
				AccessKeyID:     m.accessKey,
				SecretAccessKey: m.secretKey,
				SessionToken:    m.sessionToken,
			}, nil
		}))
		return &sarama.AccessToken{Token: token}, err

	default: // load default aws creds
		token, _, err := signer.GenerateAuthToken(ctx, m.region)
		return &sarama.AccessToken{Token: token}, err
	}
}

func (c *KafkaClients) getSyncProducer() (sarama.SyncProducer, error) {
	// Add SyncProducer specific properties to copy of base config
	c.config.Producer.RequiredAcks = sarama.WaitForAll
	c.config.Producer.Retry.Max = 5
	c.config.Producer.Return.Successes = true

	if *c.maxMessageBytes > 0 {
		c.config.Producer.MaxMessageBytes = *c.maxMessageBytes
	}

	saramaClient, err := sarama.NewClient(*c.brokers, c.config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(saramaClient)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
