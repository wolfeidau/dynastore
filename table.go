package dynastore

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	dexp "github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type contextKey int

const (
	OperationNameKey contextKey = 1 + iota

	listDefaultTimeout = time.Second * 10
)

type DynaTable struct {
	session   *DynaSession
	tableName string
}

func (dt *DynaTable) GetTableName() string {
	return dt.tableName
}

func (dt *DynaTable) Partition(partition string) *DynaPartition {
	return &DynaPartition{session: dt.session, table: dt, partition: partition}
}

// Put a value at the specified key
func (dt *DynaTable) PutWithContext(ctx context.Context, partitionKey, hashKey string, options ...WriteOption) error {
	writeOptions := NewWriteOptions(options...)

	ctx = setOperationName(ctx, "Put")

	update, err := buildUpdate(writeOptions)
	if err != nil {
		return fmt.Errorf("failed to build update: %w", err)
	}

	expr, err := dexp.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		return fmt.Errorf("failed to build update expression: %w", err)
	}

	updateItem := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(dt.GetTableName()),
		Key:                       buildKeys(partitionKey, hashKey),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	}

	ctx = dt.session.storeHooks.RequestBuilt(ctx, updateItem)

	_, err = dt.session.UpdateItemWithContext(ctx, updateItem)
	if err != nil {
		return fmt.Errorf("failed to update item: %w", err)
	}

	return nil
}

// GetWithContext a value given its key
//
// This operation uses the DynamoDB get operation which doesn't support index read options
func (dt *DynaTable) GetWithContext(ctx context.Context, partitionKey, sortKey string, options ...ReadOption) (*KVPair, error) {
	readOptions := NewReadOptions(options...)

	ctx = setOperationName(ctx, "Get")

	if readOptions.hasIndex() {
		return nil, ErrIndexNotSupported
	}

	res, err := dt.getKey(ctx, partitionKey, sortKey, readOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to get by key: %w", err)
	}
	if res.Item == nil {
		return nil, ErrKeyNotFound
	}

	// is the item expired?
	if isItemExpired(res.Item) {
		return nil, ErrKeyNotFound
	}

	item, err := DecodeItem(res.Item)
	if err != nil {
		return nil, fmt.Errorf("failed to decode item: %w", err)
	}

	return item, nil
}

// ExistsWithContext if a sort key exists in the store
//
// This operation uses the DynamoDB get operation which doesn't support index read options
func (dt *DynaTable) ExistsWithContext(ctx context.Context, partitionKey, sortKey string, options ...ReadOption) (bool, error) {
	readOptions := NewReadOptions(options...)

	ctx = setOperationName(ctx, "Exists")

	if readOptions.hasIndex() {
		return false, ErrIndexNotSupported
	}

	getItem := &dynamodb.GetItemInput{
		TableName:      aws.String(dt.GetTableName()),
		Key:            buildKeys(partitionKey, sortKey),
		ConsistentRead: aws.Bool(readOptions.consistent),
	}

	ctx = dt.session.storeHooks.RequestBuilt(ctx, getItem)

	res, err := dt.session.GetItemWithContext(ctx, getItem)
	if err != nil {
		return false, fmt.Errorf("failed to get item: %w", err)
	}

	if res.Item == nil {
		return false, nil
	}

	// is the item expired?
	if isItemExpired(res.Item) {
		return false, nil
	}

	return true, nil
}

// DeleteWithContext the value at the specified key
func (dt *DynaTable) DeleteWithContext(ctx context.Context, partitionKey, sortKey string) error {
	ctx = setOperationName(ctx, "Delete")

	deleteItem := &dynamodb.DeleteItemInput{
		TableName: aws.String(dt.GetTableName()),
		Key:       buildKeys(partitionKey, sortKey),
	}

	ctx = dt.session.storeHooks.RequestBuilt(ctx, deleteItem)

	_, err := dt.session.DeleteItemWithContext(ctx, deleteItem)
	if err != nil {
		return fmt.Errorf("failed to delete item: %w", err)
	}

	return nil
}

// ListPageWithContext the content of a given prefix
func (dt *DynaTable) ListPageWithContext(ctx context.Context, partitionKey, prefix string, options ...ReadOption) (*KVPairPage, error) {
	readOptions := NewReadOptions(options...)

	ctx = setOperationName(ctx, "ListPage")

	knames := resolveKeyAttributes(readOptions)

	key := dexp.Key(knames.partitionKey).Equal(dexp.Value(partitionKey))

	if prefix != "" {
		key = key.And(dexp.Key(knames.sortKey).BeginsWith(prefix))
	}

	expr, err := dexp.NewBuilder().WithKeyCondition(key).Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build exp: %w", err)
	}

	query := &dynamodb.QueryInput{
		TableName:                 aws.String(dt.GetTableName()),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConsistentRead:            aws.Bool(readOptions.consistent),
		Limit:                     readOptions.limit,
	}

	if readOptions.index != nil {
		query.IndexName = aws.String(readOptions.index.name)
	}

	var decodedKey map[string]*dynamodb.AttributeValue

	// avoid either a nil or empty value
	if startKey := aws.StringValue(readOptions.startKey); startKey != "" {
		decodedKey, err = decompressAndDecodeKey(startKey)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress key: %w", err)
		}

		query.ExclusiveStartKey = decodedKey
	}

	ctx = dt.session.storeHooks.RequestBuilt(ctx, query)

	res, err := dt.session.QueryWithContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %w", err)
	}

	results := make([]*KVPair, len(res.Items))

	var val *KVPair

	for n, item := range res.Items {
		val, err = DecodeItem(item)
		if err != nil {
			return nil, fmt.Errorf("failed to run decode item: %w", err)
		}

		results[n] = val
	}

	page := &KVPairPage{Keys: results}

	if len(res.LastEvaluatedKey) != 0 {
		page.LastKey, err = compressAndEncodeKey(res.LastEvaluatedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to compress key: %w", err)
		}
	}

	return page, nil
}

// AtomicPutWithContext Atomic CAS operation on a single value.
func (dt *DynaTable) AtomicPutWithContext(ctx context.Context, partitionKey, sortKey string, options ...WriteOption) (bool, *KVPair, error) {
	writeOptions := NewWriteOptions(options...)

	update, err := buildUpdate(writeOptions)
	if err != nil {
		return false, nil, fmt.Errorf("failed to build update: %w", err)
	}

	condition := updateWithConditions(writeOptions.previous)

	expr, err := dexp.NewBuilder().WithUpdate(update).WithCondition(condition).Build()
	if err != nil {
		return false, nil, fmt.Errorf("failed to build update expression: %w", err)
	}

	updateItem := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(dt.GetTableName()),
		Key:                       buildKeys(partitionKey, sortKey),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	}

	ctx = dt.session.storeHooks.RequestBuilt(setOperationName(ctx, "AtomicPut"), updateItem)

	res, err := dt.session.UpdateItemWithContext(ctx, updateItem)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				if writeOptions.previous == nil {
					return false, nil, ErrKeyExists
				}
				return false, nil, ErrKeyModified
			}
		}
		return false, nil, err
	}

	item, err := DecodeItem(res.Attributes)
	if err != nil {
		return false, nil, fmt.Errorf("failed to decode item: %w", err)
	}

	return true, item, nil
}

// AtomicDeleteWithContext delete of a single value
//
// This supports two different operations:
// * if previous is supplied assert it exists with the version supplied
// * if previous is nil then assert that the key doesn't exist
//
// FIXME: should the second case just return false, nil?
func (dt *DynaTable) AtomicDeleteWithContext(ctx context.Context, partitionKey, sortKey string, previous *KVPair) (bool, error) {
	ctx = setOperationName(ctx, "AtomicDelete")

	getRes, err := dt.getKey(ctx, partitionKey, sortKey, NewReadOptions())
	if err != nil {
		return false, err
	}

	if previous == nil && getRes.Item != nil && !isItemExpired(getRes.Item) {
		return false, ErrKeyExists
	}

	cond := dexp.Name("version").Equal(dexp.Value(previous.Version))

	expr, err := dexp.NewBuilder().WithCondition(cond).Build()
	if err != nil {
		return false, fmt.Errorf("failed to build expression: %w", err)
	}

	req := &dynamodb.DeleteItemInput{
		TableName:                 aws.String(dt.GetTableName()),
		Key:                       buildKeys(partitionKey, sortKey),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}

	ctx = dt.session.storeHooks.RequestBuilt(ctx, req)

	_, err = dt.session.DeleteItemWithContext(ctx, req)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return false, ErrKeyNotFound
			}
		}
		return false, fmt.Errorf("failed to delete item: %w", err)
	}

	return true, nil
}

func (dt *DynaTable) getKey(ctx context.Context, partitionKey, sortKey string, options *ReadOptions) (*dynamodb.GetItemOutput, error) {
	getItem := &dynamodb.GetItemInput{
		TableName:      aws.String(dt.GetTableName()),
		ConsistentRead: aws.Bool(options.consistent),
		Key:            buildKeys(partitionKey, sortKey),
	}

	ctx = dt.session.storeHooks.RequestBuilt(ctx, getItem)

	return dt.session.GetItemWithContext(ctx, getItem)
}

func buildUpdate(options *WriteOptions) (dexp.UpdateBuilder, error) {
	update := dexp.Add(dexp.Name("version"), dexp.Value(1))

	// if a value assigned
	if options.value != nil {
		update = update.Set(dexp.Name("payload"), dexp.Value(options.value))
	}

	if options.fields != nil {
		for k, v := range options.fields {
			if isReservedField(k) {
				return update, ErrReservedField
			}
			update = update.Set(dexp.Name(k), dexp.Value(v))
		}
	}

	// if a TTL assigned
	if options.ttl != nil {
		ttlVal := time.Now().Add(*options.ttl).Unix()

		update = update.Set(dexp.Name("expires"), dexp.Value(ttlVal))
	}

	return update, nil
}

type keyAttributes struct {
	partitionKey string
	sortKey      string
}

// resolveKeyAttributes using the read options resolve the name of the keys to use in the query
// including index options.
func resolveKeyAttributes(readOptions *ReadOptions) *keyAttributes {
	knames := &keyAttributes{
		partitionKey: DefaultPartitionKeyAttribute,
		sortKey:      DefaultSortKeyAttribute,
	}

	if readOptions.index != nil {
		knames.sortKey = readOptions.index.sortKeyAttribute

		if readOptions.index.indexType == indexTypeGlobal {
			knames.partitionKey = readOptions.index.partitionKeyAttribute
		}
	}

	return knames
}

func buildKeys(partition, key string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		DefaultPartitionKeyAttribute: {S: aws.String(partition)},
		DefaultSortKeyAttribute:      {S: aws.String(key)},
	}
}

func updateWithConditions(previous *KVPair) dexp.ConditionBuilder {
	if previous != nil {
		// "version = :lastRevision AND ( attribute_not_exists(expires) OR (attribute_exists(expires) AND expires > :timeNow) )"

		// the previous kv is in the DB and is at the expected revision, also if it has a TTL set it is NOT expired.
		checkExpires := dexp.Or(
			dexp.AttributeNotExists(dexp.Name("expires")),
			dexp.Name("expires").GreaterThanEqual(dexp.Value(time.Now().Unix())),
		)

		//
		// if there is a previous provided then we override the create check
		//
		checkVersion := dexp.Name("version").Equal(dexp.Value(previous.Version))

		return dexp.And(checkVersion, checkExpires)
	}

	//
	// assign the create check to ensure record doesn't exist which isn't expired
	//

	// "(attribute_not_exists(id) AND attribute_not_exists(#name)) OR (attribute_exists(expires) AND expires < :timeNow)"

	// the previous kv is in the DB and is at the expected revision, also if it has a TTL set it is NOT expired.
	checkExpires := dexp.And(
		dexp.AttributeNotExists(dexp.Name("expires")),
		dexp.Name("expires").LessThan(dexp.Value(time.Now().Unix())),
	)
	// if the record exists and is NOT expired
	checkExists := dexp.And(
		dexp.AttributeNotExists(dexp.Name(DefaultPartitionKeyAttribute)),
		dexp.AttributeNotExists(dexp.Name(DefaultSortKeyAttribute)),
	)

	return dexp.Or(checkExists, checkExpires)
}
