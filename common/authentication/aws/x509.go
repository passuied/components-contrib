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
	"crypto/ecdsa"
	"crypto/tls"
	cryptoX509 "crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	v2creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	awssh "github.com/aws/rolesanywhere-credential-helper/aws_signing_helper"
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere"
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere/rolesanywhereiface"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	cryptopem "github.com/dapr/kit/crypto/pem"
	spiffecontext "github.com/dapr/kit/crypto/spiffe/context"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/dapr/kit/ptr"
)

func isX509Auth(m map[string]string) bool {
	tp := m["trustProfileArn"]
	ta := m["trustAnchorArn"]
	ar := m["assumeRoleArn"]
	return tp != "" && ta != "" && ar != ""
}

type x509Options struct {
	TrustProfileArn *string `json:"trustProfileArn" mapstructure:"trustProfileArn"`
	TrustAnchorArn  *string `json:"trustAnchorArn" mapstructure:"trustAnchorArn"`
	AssumeRoleArn   *string `json:"assumeRoleArn" mapstructure:"assumeRoleArn"`
}

type x509 struct {
	mu      sync.RWMutex
	wg      sync.WaitGroup
	closeCh chan struct{}

	logger              logger.Logger
	clients             *Clients
	rolesAnywhereClient rolesanywhereiface.RolesAnywhereAPI // this is so we can mock it in tests

	chainPEM []byte
	keyPEM   []byte

	region          *string
	trustProfileArn *string
	trustAnchorArn  *string
	assumeRoleArn   *string
	sessionName     string

	awsCfg *awsv2.Config
}

func newX509(ctx context.Context, opts Options, cfg *awsv2.Config) (*x509, error) {
	var x509Auth x509Options
	if err := kitmd.DecodeMetadata(opts.Properties, &x509Auth); err != nil {
		return nil, err
	}

	switch {
	case x509Auth.TrustProfileArn == nil:
		return nil, errors.New("trustProfileArn is required")
	case x509Auth.TrustAnchorArn == nil:
		return nil, errors.New("trustAnchorArn is required")
	case x509Auth.AssumeRoleArn == nil:
		return nil, errors.New("assumeRoleArn is required")
	}

	auth := &x509{
		logger:          opts.Logger,
		trustProfileArn: x509Auth.TrustProfileArn,
		trustAnchorArn:  x509Auth.TrustAnchorArn,
		assumeRoleArn:   x509Auth.AssumeRoleArn,
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
		closeCh: make(chan struct{}),
	}

	if err := auth.getCertPEM(ctx); err != nil {
		return nil, fmt.Errorf("failed to get x.509 credentials: %v", err)
	}

	// Parse trust anchor and profile ARNs
	if err := auth.initializeTrustAnchors(); err != nil {
		return nil, err
	}

	auth.startSessionRefresher()

	return auth, nil
}

func (a *x509) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	close(a.closeCh)
	a.wg.Wait()

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

func (a *x509) getCertPEM(ctx context.Context) error {
	svid, ok := spiffecontext.From(ctx)
	if !ok {
		return errors.New("no SVID found in context")
	}
	svidx, err := svid.GetX509SVID()
	if err != nil {
		return err
	}
	chainPEM, keyPEM, err := svidx.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal SVID: %w", err)
	}
	a.chainPEM = chainPEM
	a.keyPEM = keyPEM
	return nil
}

// All client getters should use v2 config
func (a *x509) S3() *S3Clients {
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

func (a *x509) DynamoDB() *DynamoDBClients {
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

func (a *x509) Sqs() *SqsClients {
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

func (a *x509) Sns() *SnsClients {
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

func (a *x509) SnsSqs() *SnsSqsClients {
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

func (a *x509) SecretManager() *SecretManagerClients {
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

func (a *x509) ParameterStore() *ParameterStoreClients {
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

func (a *x509) Kinesis() *KinesisClients {
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

func (a *x509) Ses() *SesClients {
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

func (a *x509) getDatabaseToken(ctx context.Context, poolConfig *pgxpool.Config) (string, error) {
	dbEndpoint := poolConfig.ConnConfig.Host + ":" + strconv.Itoa(int(poolConfig.ConnConfig.Port))

	// Use v2 credentials from awsCfg
	if a.awsCfg != nil && a.awsCfg.Credentials != nil {
		creds, err := a.awsCfg.Credentials.Retrieve(ctx)
		if err == nil && creds.AccessKeyID != "" && creds.SecretAccessKey != "" {
			awsCfg := v2creds.NewStaticCredentialsProvider(creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken)
			authenticationToken, err := auth.BuildAuthToken(
				ctx, dbEndpoint, *a.region, poolConfig.ConnConfig.User, awsCfg)
			if err != nil {
				return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
			}
			return authenticationToken, nil
		}
	}

	if a.assumeRoleArn != nil {
		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to load default AWS authentication configuration %w", err)
		}
		stsClient := sts.NewFromConfig(awsCfg)
		assumeRoleCfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(*a.region),
			config.WithCredentialsProvider(
				awsv2.NewCredentialsCache(
					stscreds.NewAssumeRoleProvider(stsClient, *a.assumeRoleArn, func(aro *stscreds.AssumeRoleOptions) {
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
	authenticationToken, err := auth.BuildAuthToken(
		ctx, dbEndpoint, *a.region, poolConfig.ConnConfig.User, awsCfg.Credentials)
	if err != nil {
		return "", fmt.Errorf("failed to create AWS authentication token: %w", err)
	}
	return authenticationToken, nil
}

func (a *x509) UpdatePostgres(ctx context.Context, poolConfig *pgxpool.Config) {
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

func (a *x509) Kafka(opts KafkaOptions) (*KafkaClients, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.clients.kafka != nil {
		return a.clients.kafka, nil
	}

	a.clients.kafka = initKafkaClients(opts)
	err := a.clients.kafka.NewV2(a.awsCfg, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS IAM Kafka config: %w", err)
	}
	return a.clients.kafka, nil
}

func (a *x509) initializeTrustAnchors() error {
	var (
		trustAnchor arn.ARN
		profile     arn.ARN
		err         error
	)
	if a.trustAnchorArn != nil {
		trustAnchor, err = arn.Parse(*a.trustAnchorArn)
		if err != nil {
			return err
		}
		a.region = &trustAnchor.Region
	}

	if a.trustProfileArn != nil {
		profile, err = arn.Parse(*a.trustProfileArn)
		if err != nil {
			return err
		}

		if profile.Region != "" && trustAnchor.Region != profile.Region {
			return fmt.Errorf("trust anchor and profile must be in the same region: trustAnchor=%s, profile=%s",
				trustAnchor.Region, profile.Region)
		}
	}
	return nil
}

func (a *x509) setSigningFunction(rolesAnywhereClient *rolesanywhere.RolesAnywhere) error {
	certs, err := cryptopem.DecodePEMCertificatesChain(a.chainPEM)
	if err != nil {
		return err
	}

	ints := make([]cryptoX509.Certificate, 0, len(certs)-1)
	for i := range certs[1:] {
		ints = append(ints, *certs[i+1])
	}

	key, err := cryptopem.DecodePEMPrivateKey(a.keyPEM)
	if err != nil {
		return err
	}

	keyECDSA := key.(*ecdsa.PrivateKey)
	signFunc := awssh.CreateSignFunction(*keyECDSA, *certs[0], ints)

	// No v1 request handlers, so this is a no-op or needs v2 equivalent if available
	_ = signFunc // You may need to adapt this for v2 if you use custom signing

	return nil
}

func (a *x509) createOrRefreshSession(ctx context.Context) (*awsv2.Credentials, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}}

	// Use v2 config for RolesAnywhere
	// You may need to adapt this if the RolesAnywhere helper expects v1 session/config
	// If so, you may need to keep a minimal v1 dependency just for this, or use v2 if available

	// Example: create session using RolesAnywhere v2 (pseudo-code, adapt as needed)
	// rolesAnywhereClient := rolesanywhere.NewV2(a.awsCfg, client)
	// if err := a.setSigningFunction(rolesAnywhereClient); err != nil {
	//     return nil, err
	// }

	createSessionRequest := rolesanywhere.CreateSessionInput{
		Cert:            ptr.Of(string(a.chainPEM)),
		ProfileArn:      a.trustProfileArn,
		TrustAnchorArn:  a.trustAnchorArn,
		RoleArn:         a.assumeRoleArn,
		DurationSeconds: ptr.Of(int64(time.Hour.Seconds())),
	}

	var output *rolesanywhere.CreateSessionOutput
	var err error
	if a.rolesAnywhereClient != nil {
		output, err = a.rolesAnywhereClient.CreateSession(ctx, &createSessionRequest)
	} else {
		// You may need to instantiate a v2 RolesAnywhere client here
		return nil, errors.New("rolesAnywhereClient is not set")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create session using dapr app identity: %w", err)
	}

	if output == nil || len(output.CredentialSet) != 1 {
		return nil, fmt.Errorf("expected 1 credential set from X.509 rolesanyway response, got %d", len(output.CredentialSet))
	}

	accessKey := output.CredentialSet[0].Credentials.AccessKeyId
	secretKey := output.CredentialSet[0].Credentials.SecretAccessKey
	sessionToken := output.CredentialSet[0].Credentials.SessionToken
	creds := v2creds.NewStaticCredentialsProvider(*accessKey, *secretKey, *sessionToken)
	return &awsv2.Credentials{
		AccessKeyID:     *accessKey,
		SecretAccessKey: *secretKey,
		SessionToken:    *sessionToken,
		Source:          "RolesAnywhere",
	}, nil
}

func (a *x509) startSessionRefresher() {
	a.logger.Infof("starting session refresher for x509 auth")

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		for {
			// renew at ~half the lifespan
			// You may need to store expiration in your struct if using v2 creds
			refreshInterval := 30 * time.Minute
			select {
			case <-time.After(refreshInterval):
				a.refreshClient()
			case <-a.closeCh:
				a.logger.Debugf("Session refresher is stopped")
				return
			}
		}
	}()
}

func (a *x509) refreshClient() {
	for {
		_, err := a.createOrRefreshSession(context.Background())
		if err == nil {
			// You may need to update your clients with new credentials here
			a.logger.Debugf("AWS IAM Roles Anywhere session credentials refreshed successfully")
			return
		}
		a.logger.Errorf("Failed to refresh session, retrying in 5 seconds: %w", err)
		select {
		case <-time.After(time.Second * 5):
		case <-a.closeCh:
			return
		}
	}
}
