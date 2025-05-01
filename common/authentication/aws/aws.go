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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dapr/kit/logger"
)

type EnvironmentSettings struct {
	Metadata map[string]string
}

// TODO: Delete in Dapr 1.17 so we can move all IAM fields to use the defaults of:
// accessKey and secretKey and region as noted in the docs, and Options struct above.
type DeprecatedKafkaIAM struct {
	Region         string `json:"awsRegion" mapstructure:"awsRegion"`
	AccessKey      string `json:"awsAccessKey" mapstructure:"awsAccessKey"`
	SecretKey      string `json:"awsSecretKey" mapstructure:"awsSecretKey"`
	SessionToken   string `json:"awsSessionToken" mapstructure:"awsSessionToken"`
	IamRoleArn     string `json:"awsIamRoleArn" mapstructure:"awsIamRoleArn"`
	StsSessionName string `json:"awsStsSessionName" mapstructure:"awsStsSessionName"`
}

type Options struct {
	Logger     logger.Logger
	Properties map[string]string

	PoolConfig       *pgxpool.Config `json:"poolConfig" mapstructure:"poolConfig"`
	ConnectionString string          `json:"connectionString" mapstructure:"connectionString"`

	// TODO: in Dapr 1.17 rm the alias on regions as we rm the aws prefixed one.
	// Docs have it just as region, but most metadata fields show the aws prefix...
	Region        string `json:"region" mapstructure:"region" mapstructurealiases:"awsRegion"`
	AccessKey     string `json:"accessKey" mapstructure:"accessKey"`
	SecretKey     string `json:"secretKey" mapstructure:"secretKey"`
	SessionName   string `json:"sessionName" mapstructure:"sessionName"`
	AssumeRoleARN string `json:"assumeRoleArn" mapstructure:"assumeRoleArn"`
	SessionToken  string `json:"sessionToken" mapstructure:"sessionToken"`

	Endpoint string
}

// TODO: Delete in Dapr 1.17 so we can move all IAM fields to use the defaults of:
// accessKey and secretKey and region as noted in the docs, and Options struct above.
type DeprecatedPostgresIAM struct {
	// Access key to use for accessing PostgreSQL.
	AccessKey string `json:"awsAccessKey" mapstructure:"awsAccessKey"`
	// Secret key to use for accessing PostgreSQL.
	SecretKey string `json:"awsSecretKey" mapstructure:"awsSecretKey"`
}

func GetConfig(opts Options) *aws.Config {
	cfg := aws.NewConfig()

	if opts.Region != "" {
		cfg.Region = opts.Region
	}
	if opts.Endpoint != "" {
		cfg.EndpointResolverWithOptions = aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: opts.Endpoint, SigningRegion: opts.Region}, nil
			})
	}

	return cfg
}

//nolint:interfacebloat
type Provider interface {
	S3() *S3Clients
	DynamoDB() *DynamoDBClients
	Sqs() *SqsClients
	Sns() *SnsClients
	SnsSqs() *SnsSqsClients
	SecretManager() *SecretManagerClients
	ParameterStore() *ParameterStoreClients
	Kinesis() *KinesisClients
	Ses() *SesClients
	Kafka(KafkaOptions) (*KafkaClients, error)

	// Postgres is an outlier to the others in the sense that we can update only it's config,
	// as we use a max connection time of 8 minutes.
	// This means that we can just update the config session credentials,
	// and then in 8 minutes it will update to a new session automatically for us.
	UpdatePostgres(context.Context, *pgxpool.Config)

	Close() error
}

func NewProvider(ctx context.Context, opts Options, cfg *aws.Config) (Provider, error) {
	if isX509Auth(opts.Properties) {
		return newX509(ctx, opts, cfg)
	}
	return newStaticIAM(ctx, opts, cfg)
}

// NewEnvironmentSettings returns a new EnvironmentSettings configured for a given AWS resource.
func NewEnvironmentSettings(md map[string]string) (EnvironmentSettings, error) {
	es := EnvironmentSettings{
		Metadata: md,
	}

	return es, nil
}

// Coalesce is a helper function to return the first non-empty string from the inputs
// This helps us to migrate away from the deprecated duplicate aws auth profile metadata fields in Dapr 1.17.
func Coalesce(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// MockParameterStore for v2
type MockParameterStore struct {
	GetParameterFn       func(context.Context, *ssm.GetParameterInput, ...func(*ssm.Options)) (*ssm.GetParameterOutput, error)
	DescribeParametersFn func(context.Context, *ssm.DescribeParametersInput, ...func(*ssm.Options)) (*ssm.DescribeParametersOutput, error)
}

func (m *MockParameterStore) GetParameter(ctx context.Context, input *ssm.GetParameterInput, optFns ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
	return m.GetParameterFn(ctx, input, optFns...)
}

func (m *MockParameterStore) DescribeParameters(ctx context.Context, input *ssm.DescribeParametersInput, optFns ...func(*ssm.Options)) (*ssm.DescribeParametersOutput, error) {
	return m.DescribeParametersFn(ctx, input, optFns...)
}

// MockSecretManager for v2
type MockSecretManager struct {
	GetSecretValueFn func(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

func (m *MockSecretManager) GetSecretValue(ctx context.Context, input *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	return m.GetSecretValueFn(ctx, input, optFns...)
}

// MockDynamoDB for v2
type MockDynamoDB struct {
	GetItemFn            func(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItemFn            func(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItemFn         func(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	BatchWriteItemFn     func(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
	TransactWriteItemsFn func(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

func (m *MockDynamoDB) GetItem(ctx context.Context, input *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return m.GetItemFn(ctx, input, optFns...)
}

func (m *MockDynamoDB) PutItem(ctx context.Context, input *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return m.PutItemFn(ctx, input, optFns...)
}

func (m *MockDynamoDB) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return m.DeleteItemFn(ctx, input, optFns...)
}

func (m *MockDynamoDB) BatchWriteItem(ctx context.Context, input *dynamodb.BatchWriteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return m.BatchWriteItemFn(ctx, input, optFns...)
}

func (m *MockDynamoDB) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return m.TransactWriteItemsFn(ctx, input, optFns...)
}
