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

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/aws/aws-sdk-go/service/ssm/ssmiface"
)

type MockParameterStore struct {
	GetParameterFn       func(context.Context, *ssm.GetParameterInput, ...request.Option) (*ssm.GetParameterOutput, error)
	DescribeParametersFn func(context.Context, *ssm.DescribeParametersInput, ...request.Option) (*ssm.DescribeParametersOutput, error)
	ssmiface.SSMAPI
}

func (m *MockParameterStore) GetParameterWithContext(ctx context.Context, input *ssm.GetParameterInput, option ...request.Option) (*ssm.GetParameterOutput, error) {
	return m.GetParameterFn(ctx, input, option...)
}

func (m *MockParameterStore) DescribeParametersWithContext(ctx context.Context, input *ssm.DescribeParametersInput, option ...request.Option) (*ssm.DescribeParametersOutput, error) {
	return m.DescribeParametersFn(ctx, input, option...)
}

type MockSecretManager struct {
	GetSecretValueFn func(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
	ListSecretsFn    func(context.Context, *secretsmanager.ListSecretsInput, ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretsOutput, error)
}

func (m *MockSecretManager) GetSecretValue(ctx context.Context, input *secretsmanager.GetSecretValueInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	return m.GetSecretValueFn(ctx, input, opts...)
}

func (m *MockSecretManager) ListSecrets(ctx context.Context, input *secretsmanager.ListSecretsInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretsOutput, error) {
	return m.ListSecretsFn(ctx, input, opts...)
}

type MockDynamoDB struct {
	GetItemFn            func(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItemFn            func(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItemFn         func(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	TransactWriteItemsFn func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
	BatchWriteItemFn     func(ctx context.Context, input *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

func (m *MockDynamoDB) GetItem(ctx context.Context, input *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return m.GetItemFn(ctx, input, opts...)
}

func (m *MockDynamoDB) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return m.PutItemFn(ctx, input, opts...)
}

func (m *MockDynamoDB) BatchWriteItem(ctx context.Context, input *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	return m.BatchWriteItemFn(ctx, input, opts...)
}

func (m *MockDynamoDB) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return m.DeleteItemFn(ctx, input, opts...)
}

func (m *MockDynamoDB) TransactWriteItems(ctx context.Context, input *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return m.TransactWriteItemsFn(ctx, input, opts...)
}
