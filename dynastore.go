package dynastore

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	// ErrKeyNotFound record not found in the table
	ErrKeyNotFound = errors.New("key not found in table")

	// ErrKeyExists record already exists in table
	ErrKeyExists = errors.New("key already exists in table")

	// ErrKeyModified record has been modified, this probably means someone beat you to the change/lock
	ErrKeyModified = errors.New("key has been modified")

	// ErrReservedField put contained an field in the write options which was reserved
	ErrReservedField = errors.New("fields contained reserved attribute name")

	// ErrIndexNotSupported dynamodb get operations don't support specifying an index
	ErrIndexNotSupported = errors.New("indexes not supported for this operation")
)

// Session represents the backend K/V storage using one or more DynamoDB tables containing partitions.
// This primarily holds the AWS Session settings and configuration, and enables direct access to DynamoDB.
type Session interface {
	dynamodbiface.DynamoDBAPI

	// Table returns a table
	Table(tableName string) Table
}

// Table represents a table in DynamoDB, this is where you store all your partitioned data for a given
// model.
type Table interface {
	GetTableName() string

	Partition(partitionName string) Partition

	PutWithContext(ctx context.Context, partitionKey, sortKey string, options ...WriteOption) error

	GetWithContext(ctx context.Context, partitionKey, sortKey string, options ...ReadOption) (*KVPair, error)

	ListPageWithContext(ctx context.Context, partitionKey, prefix string, options ...ReadOption) (*KVPairPage, error)

	DeleteWithContext(ctx context.Context, partitionKey, sortKey string) error

	ExistsWithContext(ctx context.Context, partitionKey, sortKey string, options ...ReadOption) (bool, error)

	AtomicPutWithContext(ctx context.Context, partitionKey, sortKey string, options ...WriteOption) (bool, *KVPair, error)

	AtomicDeleteWithContext(ctx context.Context, partitionKey, sortKey string, previous *KVPair) (bool, error)
}

// Partition a partition represents a grouping of data within a DynamoDB table.
type Partition interface {
	GetPartitionName() string

	Put(sortKey string, options ...WriteOption) error

	PutWithContext(ctx context.Context, sortKey string, options ...WriteOption) error

	Get(key string, options ...ReadOption) (*KVPair, error)

	GetWithContext(ctx context.Context, sortKey string, options ...ReadOption) (*KVPair, error)

	List(prefix string, options ...ReadOption) ([]*KVPair, error)

	ListWithContext(ctx context.Context, prefix string, options ...ReadOption) ([]*KVPair, error)

	ListPage(prefix string, options ...ReadOption) (*KVPairPage, error)

	ListPageWithContext(ctx context.Context, prefix string, options ...ReadOption) (*KVPairPage, error)

	Delete(sortKey string) error

	DeleteWithContext(ctx context.Context, sortKey string) error

	Exists(sortKey string, options ...ReadOption) (bool, error)

	ExistsWithContext(ctx context.Context, sortKey string, options ...ReadOption) (bool, error)

	AtomicPut(sortKey string, options ...WriteOption) (bool, *KVPair, error)

	AtomicPutWithContext(ctx context.Context, sortKey string, options ...WriteOption) (bool, *KVPair, error)

	AtomicDelete(sortKey string, previous *KVPair) (bool, error)

	AtomicDeleteWithContext(ctx context.Context, sortKey string, previous *KVPair) (bool, error)
}

// StoreHooks is a container for callbacks that can instrument the datastore
type StoreHooks struct {
	// RequestBuilt will be invoked prior to dispatching the request to the AWS SDK
	RequestBuilt func(ctx context.Context, params interface{}) context.Context
}

var defaultHooks = &StoreHooks{
	RequestBuilt: func(ctx context.Context, params interface{}) context.Context {
		return ctx
	},
}
