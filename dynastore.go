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
	// GetTableName return the name of the table being used
	GetTableName() string

	// Partition returns a partition store
	Partition(partitionName string) Partition
}

// Partition a partition represents a grouping of data within a DynamoDB table.
type Partition interface {

	// GetPartitionName return the name of the partition being used
	GetPartitionName() string

	// Put a value at the specified key
	Put(key string, options ...WriteOption) error

	// Put a value at the specified key
	PutWithContext(ctx context.Context, key string, options ...WriteOption) error

	// Get a value given its key
	Get(key string, options ...ReadOption) (*KVPair, error)

	// Get a value given its key
	GetWithContext(ctx context.Context, key string, options ...ReadOption) (*KVPair, error)

	// List the content of a given prefix
	List(prefix string, options ...ReadOption) ([]*KVPair, error)

	// List the content of a given prefix
	ListWithContext(ctx context.Context, prefix string, options ...ReadOption) ([]*KVPair, error)

	// List the content of the given prefix and return a page which contains the key
	// and includes a last key if there were more records.
	//
	// The ReadWithStartKey can be used to pass the key to the next call.
	ListPage(prefix string, options ...ReadOption) (*KVPairPage, error)

	// List the content of the given prefix and return a page which contains the key
	// and includes a last key if there were more records.
	//
	// The ReadWithStartKey can be used to pass the key to the next call.
	ListPageWithContext(ctx context.Context, prefix string, options ...ReadOption) (*KVPairPage, error)

	// Delete the value at the specified key
	Delete(key string) error

	// Delete the value at the specified key
	DeleteWithContext(ctx context.Context, key string) error

	// Verify if a Key exists in the store
	Exists(key string, options ...ReadOption) (bool, error)

	// Verify if a Key exists in the store
	ExistsWithContext(ctx context.Context, key string, options ...ReadOption) (bool, error)

	// Atomic CAS operation on a single value.
	// Pass previous = nil to create a new key.
	// Pass previous = kv to update an existing value.
	AtomicPut(key string, options ...WriteOption) (bool, *KVPair, error)

	// Atomic CAS operation on a single value.
	// Pass previous = nil to create a new key.
	// Pass previous = kv to update an existing value.
	AtomicPutWithContext(ctx context.Context, key string, options ...WriteOption) (bool, *KVPair, error)

	// Atomic delete of a single value
	AtomicDelete(key string, previous *KVPair) (bool, error)

	// Atomic delete of a single value
	AtomicDeleteWithContext(ctx context.Context, key string, previous *KVPair) (bool, error)
}
