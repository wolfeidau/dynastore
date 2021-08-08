package dynastore

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// dynaPartition store which is backed by AWS DynamoDB
type DynaPartition struct {
	session   *DynaSession
	table     Table
	partition string
}

func (ddb *DynaPartition) GetTableName() string {
	return ddb.table.GetTableName()
}

func (ddb *DynaPartition) GetPartitionName() string {
	return ddb.partition
}

// Put a value at the specified key
func (ddb *DynaPartition) Put(hashKey string, options ...WriteOption) error {
	return ddb.PutWithContext(context.Background(), hashKey, options...)
}

// Put a value at the specified key
func (ddb *DynaPartition) PutWithContext(ctx context.Context, hashKey string, options ...WriteOption) error {
	return ddb.table.PutWithContext(ctx, ddb.partition, hashKey, options...)
}

// Exists if a sort key exists in the store
func (ddb *DynaPartition) Exists(sortKey string, options ...ReadOption) (bool, error) {
	return ddb.ExistsWithContext(context.Background(), sortKey, options...)
}

// Exists if a sort key exists in the store
func (ddb *DynaPartition) ExistsWithContext(ctx context.Context, sortKey string, options ...ReadOption) (bool, error) {
	return ddb.table.ExistsWithContext(ctx, ddb.partition, sortKey, options...)
}

// Get a value given its sort key
func (ddb *DynaPartition) Get(sortKey string, options ...ReadOption) (*KVPair, error) {
	return ddb.GetWithContext(context.Background(), sortKey, options...)
}

// Get a value given its key
func (ddb *DynaPartition) GetWithContext(ctx context.Context, sortKey string, options ...ReadOption) (*KVPair, error) {
	return ddb.table.GetWithContext(ctx, ddb.partition, sortKey, options...)
}

// Delete the value at the specified key
func (ddb *DynaPartition) Delete(sortKey string) error {
	return ddb.DeleteWithContext(context.Background(), sortKey)
}

// Delete the value at the specified key
func (ddb *DynaPartition) DeleteWithContext(ctx context.Context, sortKey string) error {
	return ddb.table.DeleteWithContext(ctx, ddb.partition, sortKey)
}

// List the content of a given prefix
func (ddb *DynaPartition) ListPage(prefix string, options ...ReadOption) (*KVPairPage, error) {
	return ddb.ListPageWithContext(context.Background(), prefix, options...)
}

// List the content of a given prefix
func (ddb *DynaPartition) ListPageWithContext(ctx context.Context, prefix string, options ...ReadOption) (*KVPairPage, error) {
	return ddb.table.ListPageWithContext(ctx, ddb.partition, prefix, options...)
}

// List the content of a given prefix
//
// Deprecated: This function attempts to list all records using a deadline / timeout which turned out to be
// a bad idea, use ListPage
func (ddb *DynaPartition) List(prefix string, options ...ReadOption) ([]*KVPair, error) {
	return ddb.ListWithContext(context.Background(), prefix, options...)
}

// List the content of a given prefix
//
// Deprecated: This function attempts to list all records using a deadline / timeout which turned out to be
// a bad idea, use ListPageWithContext
func (ddb *DynaPartition) ListWithContext(ctx context.Context, prefix string, options ...ReadOption) ([]*KVPair, error) {
	readOptions := NewReadOptions(options...)

	ctx = setOperationName(ctx, "List")

	query := &dynamodb.QueryInput{
		TableName:              aws.String(ddb.GetTableName()),
		KeyConditionExpression: aws.String("#id = :partition AND begins_with(#name, :namePrefix)"),
		ExpressionAttributeNames: map[string]*string{
			"#id":   aws.String("id"),
			"#name": aws.String("name"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":partition":  {S: aws.String(ddb.partition)},
			":namePrefix": {S: aws.String(prefix)},
		},
		ConsistentRead: aws.Bool(readOptions.consistent),
	}

	ctx, cancel := context.WithTimeout(ctx, listDefaultTimeout)

	var items []map[string]*dynamodb.AttributeValue

	ctx = ddb.session.storeHooks.RequestBuilt(ctx, query)

	err := ddb.session.QueryPagesWithContext(ctx, query,
		func(page *dynamodb.QueryOutput, lastPage bool) bool {
			items = append(items, page.Items...)

			if lastPage {
				cancel()
				return false
			}

			return true
		})
	if err != nil {
		return nil, fmt.Errorf("failed to query table: %w", err)
	}

	if len(items) == 0 {
		return nil, ErrKeyNotFound
	}

	var results []*KVPair

	for _, item := range items {
		val, err := DecodeItem(item)
		if err != nil {
			return nil, fmt.Errorf("failed to decode item: %w", err)
		}

		// skip records which are expired
		if isItemExpired(item) {
			continue
		}

		results = append(results, val)
	}

	return results, nil
}

// AtomicPut Atomic CAS operation on a single value.
func (ddb *DynaPartition) AtomicPut(sortKey string, options ...WriteOption) (bool, *KVPair, error) {
	return ddb.AtomicPutWithContext(context.Background(), sortKey, options...)
}

// AtomicPut Atomic CAS operation on a single value.
func (ddb *DynaPartition) AtomicPutWithContext(ctx context.Context, sortKey string, options ...WriteOption) (bool, *KVPair, error) {
	return ddb.table.AtomicPutWithContext(ctx, ddb.partition, sortKey, options...)
}

// AtomicDelete delete of a single value
//
// This supports two different operations:
// * if previous is supplied assert it exists with the version supplied
// * if previous is nil then assert that the key doesn't exist
//
func (ddb *DynaPartition) AtomicDelete(sortKey string, previous *KVPair) (bool, error) {
	return ddb.AtomicDeleteWithContext(context.Background(), sortKey, previous)
}

// AtomicDelete delete of a single value
//
// This supports two different operations:
// * if previous is supplied assert it exists with the version supplied
// * if previous is nil then assert that the key doesn't exist
//
// FIXME: should the second case just return false, nil?
func (ddb *DynaPartition) AtomicDeleteWithContext(ctx context.Context, sortKey string, previous *KVPair) (bool, error) {
	return ddb.table.AtomicDeleteWithContext(ctx, ddb.partition, sortKey, previous)
}
