package dynastore

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynaSession struct {
	*dynamodb.DynamoDB
	storeHooks *StoreHooks
}

func (ds *DynaSession) Table(tableName string) *DynaTable {
	return &DynaTable{session: ds, tableName: tableName}
}

// New construct a DynamoDB backed store with default session / service
func New(cfgs ...*aws.Config) *DynaSession {
	sess := session.Must(session.NewSession(cfgs...))
	dynamoSvc := dynamodb.New(sess)

	return &DynaSession{
		dynamoSvc, defaultHooks,
	}
}

// New construct a DynamoDB backed store with default session / service
func NewWithOptions(awscfg *aws.Config, options ...SessionOption) *DynaSession {
	sessionOptions := NewSessionOptions(options...)

	sess := session.Must(session.NewSession(awscfg))
	dynamoSvc := dynamodb.New(sess)

	return &DynaSession{
		dynamoSvc,
		sessionOptions.storeHooks,
	}
}

func NewWithClient(dynamoSvc *dynamodb.DynamoDB, storeHooks *StoreHooks) *DynaSession {
	return &DynaSession{
		dynamoSvc,
		storeHooks,
	}
}
