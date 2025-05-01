/*
Copyright 2021 The Dapr Authors
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
	"strconv"
	"sync"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	v2creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/kit/logger"
)

type StaticAuth struct {
	mu     sync.RWMutex
	logger logger.Logger

	region       *string
	endpoint     *string
	accessKey    *string
	secretKey    *string
	sessionToken string

	assumeRoleARN *string
	sessionName   string

	awsCfg  *awsv2.Config
	clients *Clients
}

func newStaticIAM(ctx context.Context, opts Options, cfg *awsv2.Config) (*StaticAuth, error) {
	auth := &StaticAuth{
		logger: opts.Logger,
		awsCfg: func() *awsv2.Config {
			if cfg != nil {
				return cfg
			}
			c, err := GetConfigV2(opts.AccessKey, opts.SecretKey, opts.SessionToken, opts.Region, opts.Endpoint)
			if err != nil {
				return nil
			}
			return &c
		}(),
		clients: newClients(),
	}

	if opts.Region != "" {
		auth.region = &opts.Region
	}
	if opts.Endpoint != "" {
		auth.endpoint = &opts.Endpoint
	}
	if opts.AccessKey != "" {
		auth.accessKey = &opts.AccessKey
	}
	if opts.SecretKey != "" {
		auth.secretKey = &opts.SecretKey
	}
	if opts.SessionToken != "" {
		auth.sessionToken = opts.SessionToken
	}
	if opts.AssumeRoleARN != "" {
		auth.assumeRoleARN = &opts.AssumeRoleARN
	}
	if opts.SessionName != "" {
		auth.sessionName = opts.SessionName
	}

	return auth, nil
}

// This is to be used only for test purposes to inject mocked clients
func (a *StaticAuth) WithMockClients(clients *Clients) {
	a.clients = clients
}

// The following methods assume you have v2 client constructors in your Clients struct
func (a *StaticAuth) S3() *S3Clients {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clients.s3 != nil {
		return a.clients.s3
	}
	s3Clients := S3Clients{}
	s3Clients.NewV2(a.awsCfg)
	a.clients.s3 = &s3Clients
	return a.clients.s3
}

func (a *StaticAuth) DynamoDB() *DynamoDBClients {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clients.Dynamo != nil {
		return a.clients.Dynamo
	}
	clients := DynamoDBClients{}
	clients.NewV2(a.awsCfg)
	a.clients.Dynamo = &clients
	return a.clients.Dynamo
}

func (a *StaticAuth) Sqs() *SqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clients.sqs != nil {
		return a.clients.sqs
	}
	clients := SqsClients{}
	clients.NewV2(a.awsCfg)
	a.clients.sqs = &clients
	return a.clients.sqs
}

func (a *StaticAuth) Sns() *SnsClients {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clients.sns != nil {
		return a.clients.sns
	}
	clients := SnsClients{}
	clients.NewV2(a.awsCfg)
	a.clients.sns = &clients
	return a.clients.sns
}

func (a *StaticAuth) SnsSqs() *SnsSqsClients {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clients.snssqs != nil {
		return a.clients.snssqs
	}
	clients := SnsSqsClients{}
	clients.NewV2(a.awsCfg)
	a.clients.snssqs = &clients
	return a.clients.snssqs
}

func (a *StaticAuth) SecretManager() *SecretManagerClients {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clients.Secret != nil {
		return a.clients.Secret
	}
	clients := SecretManagerClients{}
	clients.NewV2(a.awsCfg)
	a.clients.Secret = &clients
	return a.clients.Secret
}

func (a *StaticAuth) ParameterStore() *ParameterStoreClients {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clients.ParameterStore != nil {
		return a.clients.ParameterStore
	}
	clients := ParameterStoreClients{}
	clients.NewV2(a.awsCfg)
	a.clients.ParameterStore = &clients
	return a.clients.ParameterStore
}

func (a *StaticAuth) Kinesis() *KinesisClients {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clients.kinesis != nil {
		return a.clients.kinesis
	}
	clients := KinesisClients{}
	clients.NewV2(a.awsCfg)
	a.clients.kinesis = &clients
	return a.clients.kinesis
}

func (a *StaticAuth) Ses() *SesClients {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.clients.ses != nil {
		return a.clients.ses
	}
	clients := SesClients{}
	clients.NewV2(a.awsCfg)
	a.clients.ses = &clients
	return a.clients.ses
}

func (a *StaticAuth) UpdatePostgres(ctx context.Context, poolConfig *pgxpool.Config) {
	a.mu.Lock()
	defer a.mu.Unlock()

	poolConfig.MaxConnLifetime = time.Minute * 8

	poolConfig.BeforeConnect = func(ctx context.Context, pgConfig *pgx.ConnConfig) error {
		pwd, err := a.getDatabaseToken(ctx, poolConfig)
		if err != nil {
			return fmt.Errorf("failed to get database token: %w", err)
		}
		pgConfig.Password = pwd
		poolConfig.ConnConfig.Password = pwd
		return nil
	}
}

func (a *StaticAuth) getDatabaseToken(ctx context.Context, poolConfig *pgxpool.Config) (string, error) {
	dbEndpoint := poolConfig.ConnConfig.Host + ":" + strconv.Itoa(int(poolConfig.ConnConfig.Port))

	if a.accessKey != nil && a.secretKey != nil {
		awsCfg := v2creds.NewStaticCredentialsProvider(*a.accessKey, *a.secretKey, a.sessionToken)
		authenticationToken, err := auth.BuildAuthToken(
			ctx, dbEndpoint, *a.region, poolConfig.ConnConfig.User, awsCfg)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
		}
		return authenticationToken, nil
	}

	if a.assumeRoleARN != nil {
		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to load default AWS authentication configuration %w", err)
		}
		stsClient := sts.NewFromConfig(awsCfg)
		assumeRoleCfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(*a.region),
			config.WithCredentialsProvider(
				awsv2.NewCredentialsCache(
					stscreds.NewAssumeRoleProvider(stsClient, *a.assumeRoleARN, func(aro *stscreds.AssumeRoleOptions) {
						if a.sessionName != "" {
							aro.RoleSessionName = a.sessionName
						}
					}),
				),
			),
		)
		if err != nil {
			return "", fmt.Errorf("failed to assume aws role %w", err)
		}
		authenticationToken, err := auth.BuildAuthToken(
			ctx, dbEndpoint, *a.region, poolConfig.ConnConfig.User, assumeRoleCfg.Credentials)
		if err != nil {
			return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
		}
		return authenticationToken, nil
	}

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to load default AWS authentication configuration %w", err)
	}
	authenticationToken, err := auth.BuildAuthToken(ctx, dbEndpoint, *a.region, poolConfig.ConnConfig.User, awsCfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
	}
	return authenticationToken, nil
}

func (a *StaticAuth) Kafka(opts KafkaOptions) (*KafkaClients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.kafka != nil {
		return a.clients.kafka, nil
	}

	a.clients.kafka = initKafkaClients(opts)
	tokenProvider := mskTokenProvider{}
	if a.assumeRoleARN != nil {
		tokenProvider.awsIamRoleArn = *a.assumeRoleARN
	}
	if a.sessionName != "" {
		tokenProvider.awsStsSessionName = a.sessionName
	}

	err := a.clients.kafka.NewV2(a.awsCfg, &tokenProvider)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS IAM Kafka config: %w", err)
	}

	return a.clients.kafka, nil
}

func (a *StaticAuth) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	errs := make([]error, 2)
	if a.clients.kafka != nil {
		if a.clients.kafka.Producer != nil {
			errs[0] = a.clients.kafka.Producer.Close()
			a.clients.kafka.Producer = nil
		}
		if a.clients.kafka.ConsumerGroup != nil {
			errs[1] = a.clients.kafka.ConsumerGroup.Close()
			a.clients.kafka.ConsumerGroup = nil
		}
	}
	return errors.Join(errs...)
}

func GetConfigV2(accessKey string, secretKey string, sessionToken string, region string, endpoint string) (awsv2.Config, error) {
	optFns := []func(*config.LoadOptions) error{}
	if region != "" {
		optFns = append(optFns, config.WithRegion(region))
	}
	if accessKey != "" && secretKey != "" {
		provider := v2creds.NewStaticCredentialsProvider(accessKey, secretKey, sessionToken)
		optFns = append(optFns, config.WithCredentialsProvider(provider))
	}
	awsCfg, err := config.LoadDefaultConfig(context.Background(), optFns...)
	if err != nil {
		return awsv2.Config{}, err
	}
	if endpoint != "" {
		awsCfg.BaseEndpoint = &endpoint
	}
	return awsCfg, nil
}
