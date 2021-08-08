package dynastore

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	dexp "github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type contextKey int

const (
	OperationNameKey contextKey = 1 + iota

	listDefaultTimeout = time.Second * 10
)

var (
	reservedFields = map[string]string{"id": "S", "name": "S", "version": "N", "expires": "N", "payload": "A"}
)

// KVPairPage provides a page of keys with next token
// to enable paging
type KVPairPage struct {
	Keys    []*KVPair `json:"keys"`
	LastKey string    `json:"last_key"`
}

// KVPair represents {Key, Value, Version} tuple, internally
// this uses a *dynamodb.AttributeValue which can be used to
// store strings, slices or structs
type KVPair struct {
	Partition string `dynamodbav:"id"`
	Key       string `dynamodbav:"name"`
	Version   int64  `dynamodbav:"version"`
	Expires   int64  `dynamodbav:"expires"`
	// handled separately to enable an number of stored values
	value  *dynamodb.AttributeValue
	fields map[string]*dynamodb.AttributeValue
}

// BytesValue use the attribute to return a slice of bytes, a nil will be returned if it is empty or nil
func (kv *KVPair) BytesValue() []byte {
	var buf []byte

	err := dynamodbattribute.Unmarshal(kv.value, &buf)
	if err != nil {
		return nil
	}

	return buf
}

// StringValue use the attribute to return a slice of bytes, an empty string will be returned if it is empty or nil
func (kv *KVPair) StringValue() string {
	var str string

	err := dynamodbattribute.Unmarshal(kv.value, &str)
	if err != nil {
		return str
	}

	return str
}

// DecodeValue decode using dynamodbattribute
func (kv *KVPair) DecodeValue(out interface{}) error {
	return dynamodbattribute.Unmarshal(kv.value, out)
}

// DecodeFields decode the extra fields, which are typically index attributes, stored in the DynamoDB record using dynamodbattribute
func (kv *KVPair) DecodeFields(out interface{}) error {
	return dynamodbattribute.UnmarshalMap(kv.fields, out)
}

type DynaSession struct {
	*dynamodb.DynamoDB
	storeHooks *StoreHooks
}

func (ds *DynaSession) Table(tableName string) *Dynatable {
	return &Dynatable{session: ds, tableName: tableName}
}

type Dynatable struct {
	session   *DynaSession
	tableName string
}

func (dt *Dynatable) GetTableName() string {
	return dt.tableName
}

func (dt *Dynatable) Partition(partition string) Partition {
	return &DynaPartition{session: dt.session, table: dt, partition: partition}
}

// dynaPartition store which is backed by AWS DynamoDB
type DynaPartition struct {
	session   *DynaSession
	table     Table
	partition string
}

// New construct a DynamoDB backed store with default session / service
func New(cfgs ...*aws.Config) *DynaSession {
	sess := session.Must(session.NewSession(cfgs...))
	dynamoSvc := dynamodb.New(sess)

	return &DynaSession{
		dynamoSvc, nil,
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

func (ddb *DynaPartition) GetTableName() string {
	return ddb.table.GetTableName()
}

func (ddb *DynaPartition) GetPartitionName() string {
	return ddb.partition
}

// Put a value at the specified key
func (ddb *DynaPartition) Put(key string, options ...WriteOption) error {
	return ddb.PutWithContext(context.Background(), key, options...)
}

// Put a value at the specified key
func (ddb *DynaPartition) PutWithContext(ctx context.Context, key string, options ...WriteOption) error {
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
		TableName:                 aws.String(ddb.GetTableName()),
		Key:                       buildKeys(ddb.partition, key),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	}

	ctx = ddb.session.storeHooks.RequestBuilt(ctx, updateItem)

	_, err = ddb.session.UpdateItemWithContext(ctx, updateItem)
	if err != nil {
		return fmt.Errorf("failed to update item: %w", err)
	}

	return nil
}

// Exists if a Key exists in the store
func (ddb *DynaPartition) Exists(key string, options ...ReadOption) (bool, error) {
	return ddb.ExistsWithContext(context.Background(), key, options...)
}

// Exists if a Key exists in the store
func (ddb *DynaPartition) ExistsWithContext(ctx context.Context, key string, options ...ReadOption) (bool, error) {
	readOptions := NewReadOptions(options...)

	ctx = setOperationName(ctx, "Exists")

	getItem := &dynamodb.GetItemInput{
		TableName:      aws.String(ddb.GetTableName()),
		Key:            buildKeys(ddb.partition, key),
		ConsistentRead: aws.Bool(readOptions.consistent),
	}

	ctx = ddb.session.storeHooks.RequestBuilt(ctx, getItem)

	res, err := ddb.session.GetItemWithContext(ctx, getItem)
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

// Get a value given its key
func (ddb *DynaPartition) Get(key string, options ...ReadOption) (*KVPair, error) {
	return ddb.GetWithContext(context.Background(), key, options...)
}

// Get a value given its key
func (ddb *DynaPartition) GetWithContext(ctx context.Context, key string, options ...ReadOption) (*KVPair, error) {
	readOptions := NewReadOptions(options...)

	ctx = setOperationName(ctx, "Get")

	res, err := ddb.getKey(ctx, key, readOptions)
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

// Delete the value at the specified key
func (ddb *DynaPartition) Delete(key string) error {
	return ddb.DeleteWithContext(context.Background(), key)
}

// Delete the value at the specified key
func (ddb *DynaPartition) DeleteWithContext(ctx context.Context, key string) error {
	ctx = setOperationName(ctx, "Delete")

	deleteItem := &dynamodb.DeleteItemInput{
		TableName: aws.String(ddb.GetTableName()),
		Key:       buildKeys(ddb.partition, key),
	}

	ctx = ddb.session.storeHooks.RequestBuilt(ctx, deleteItem)

	_, err := ddb.session.DeleteItemWithContext(ctx, deleteItem)
	if err != nil {
		return fmt.Errorf("failed to delete item: %w", err)
	}

	return nil
}

// List the content of a given prefix
func (ddb *DynaPartition) ListPage(prefix string, options ...ReadOption) (*KVPairPage, error) {
	return ddb.ListPageWithContext(context.Background(), prefix, options...)
}

// List the content of a given prefix
func (ddb *DynaPartition) ListPageWithContext(ctx context.Context, prefix string, options ...ReadOption) (*KVPairPage, error) {
	readOptions := NewReadOptions(options...)

	ctx = setOperationName(ctx, "ListPage")

	rangeKey := "name"

	if readOptions.index != nil {
		rangeKey = readOptions.index.attribute
	}

	key := dexp.Key("id").Equal(dexp.Value(ddb.partition))

	if prefix != "" {
		key = key.And(dexp.Key(rangeKey).BeginsWith(prefix))
	}

	expr, err := dexp.NewBuilder().WithKeyCondition(key).Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build exp: %w", err)
	}

	query := &dynamodb.QueryInput{
		TableName:                 aws.String(ddb.GetTableName()),
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

	ctx = ddb.session.storeHooks.RequestBuilt(ctx, query)

	res, err := ddb.session.QueryWithContext(ctx, query)
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
func (ddb *DynaPartition) AtomicPut(key string, options ...WriteOption) (bool, *KVPair, error) {
	return ddb.AtomicPutWithContext(context.Background(), key, options...)
}

// AtomicPut Atomic CAS operation on a single value.
func (ddb *DynaPartition) AtomicPutWithContext(ctx context.Context, key string, options ...WriteOption) (bool, *KVPair, error) {
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
		TableName:                 aws.String(ddb.GetTableName()),
		Key:                       buildKeys(ddb.partition, key),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	}

	ctx = ddb.session.storeHooks.RequestBuilt(setOperationName(ctx, "AtomicPut"), updateItem)

	res, err := ddb.session.UpdateItemWithContext(ctx, updateItem)
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

// AtomicDelete delete of a single value
//
// This supports two different operations:
// * if previous is supplied assert it exists with the version supplied
// * if previous is nil then assert that the key doesn't exist
//
func (ddb *DynaPartition) AtomicDelete(key string, previous *KVPair) (bool, error) {
	return ddb.AtomicDeleteWithContext(context.Background(), key, previous)
}

// AtomicDelete delete of a single value
//
// This supports two different operations:
// * if previous is supplied assert it exists with the version supplied
// * if previous is nil then assert that the key doesn't exist
//
// FIXME: should the second case just return false, nil?
func (ddb *DynaPartition) AtomicDeleteWithContext(ctx context.Context, key string, previous *KVPair) (bool, error) {
	ctx = setOperationName(ctx, "AtomicDelete")

	getRes, err := ddb.getKey(ctx, key, NewReadOptions())
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
		TableName:                 aws.String(ddb.GetTableName()),
		Key:                       buildKeys(ddb.partition, key),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	}

	ctx = ddb.session.storeHooks.RequestBuilt(ctx, req)

	_, err = ddb.session.DeleteItemWithContext(ctx, req)
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

func (ddb *DynaPartition) getKey(ctx context.Context, key string, options *ReadOptions) (*dynamodb.GetItemOutput, error) {
	getItem := &dynamodb.GetItemInput{
		TableName:      aws.String(ddb.GetTableName()),
		ConsistentRead: aws.Bool(options.consistent),
		Key: map[string]*dynamodb.AttributeValue{
			"id":   {S: aws.String(ddb.partition)},
			"name": {S: aws.String(key)},
		},
	}

	ctx = ddb.session.storeHooks.RequestBuilt(ctx, getItem)

	return ddb.session.GetItemWithContext(ctx, getItem)
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

func buildKeys(partition, key string) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		"id":   {S: aws.String(partition)},
		"name": {S: aws.String(key)},
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
		dexp.AttributeNotExists(dexp.Name("id")),
		dexp.AttributeNotExists(dexp.Name("name")),
	)

	return dexp.Or(checkExists, checkExpires)
}
