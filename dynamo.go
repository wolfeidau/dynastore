package dynastore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	dexp "github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

const (
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

func isReservedField(s string) bool {
	_, ok := reservedFields[s]
	return ok
}

type dynaSession struct {
	dynamodbiface.DynamoDBAPI
}

func (ds *dynaSession) Table(tableName string) Table {
	return &dynatable{session: ds, tableName: tableName}
}

type dynatable struct {
	session   Session
	tableName string
}

func (dt *dynatable) GetTableName() string {
	return dt.tableName
}

func (dt *dynatable) Partition(partition string) Partition {
	return &dynaPartition{session: dt.session, table: dt, partition: partition}
}

// dynaPartition store which is backed by AWS DynamoDB
type dynaPartition struct {
	session   Session
	table     Table
	partition string
}

// New construct a DynamoDB backed store with default session / service
func New(cfgs ...*aws.Config) Session {
	sess := session.Must(session.NewSession(cfgs...))
	dynamoSvc := dynamodb.New(sess)

	return &dynaSession{
		dynamoSvc,
	}
}

func (ddb *dynaPartition) GetTableName() string {
	return ddb.table.GetTableName()
}

func (ddb *dynaPartition) GetPartitionName() string {
	return ddb.partition
}

// Put a value at the specified key
func (ddb *dynaPartition) Put(key string, options ...WriteOption) error {
	writeOptions := NewWriteOptions(options...)

	update, err := buildUpdate(writeOptions)
	if err != nil {
		return fmt.Errorf("failed to build update: %w", err)
	}

	expr, err := dexp.NewBuilder().WithUpdate(update).Build()
	if err != nil {
		return fmt.Errorf("failed to build update expression: %w", err)
	}

	_, err = ddb.session.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:                 aws.String(ddb.GetTableName()),
		Key:                       buildKeys(ddb.partition, key),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	})
	if err != nil {
		return fmt.Errorf("failed to update item: %w", err)
	}

	return nil
}

// Exists if a Key exists in the store
func (ddb *dynaPartition) Exists(key string, options ...ReadOption) (bool, error) {
	readOptions := NewReadOptions(options...)

	res, err := ddb.session.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(ddb.GetTableName()),
		Key:            buildKeys(ddb.partition, key),
		ConsistentRead: aws.Bool(readOptions.consistent),
	})

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
func (ddb *dynaPartition) Get(key string, options ...ReadOption) (*KVPair, error) {
	readOptions := NewReadOptions(options...)

	res, err := ddb.getKey(key, readOptions)
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
func (ddb *dynaPartition) Delete(key string) error {
	_, err := ddb.session.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(ddb.GetTableName()),
		Key:       buildKeys(ddb.partition, key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete item: %w", err)
	}

	return nil
}

// List the content of a given prefix
func (ddb *dynaPartition) ListPage(prefix string, options ...ReadOption) (*KVPairPage, error) {
	readOptions := NewReadOptions(options...)

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

	si := &dynamodb.QueryInput{
		TableName:                 aws.String(ddb.GetTableName()),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ConsistentRead:            aws.Bool(readOptions.consistent),
		Limit:                     readOptions.limit,
	}

	if readOptions.index != nil {
		si.IndexName = aws.String(readOptions.index.name)
	}

	var decodedKey map[string]*dynamodb.AttributeValue

	// avoid either a nil or empty value
	if startKey := aws.StringValue(readOptions.startKey); startKey != "" {
		decodedKey, err = decompressAndDecodeKey(startKey)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress key: %w", err)
		}

		si.ExclusiveStartKey = decodedKey
	}

	res, err := ddb.session.Query(si)
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
func (ddb *dynaPartition) List(prefix string, options ...ReadOption) ([]*KVPair, error) {
	readOptions := NewReadOptions(options...)

	si := &dynamodb.QueryInput{
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

	ctcx, cancel := context.WithTimeout(context.Background(), listDefaultTimeout)

	var items []map[string]*dynamodb.AttributeValue

	err := ddb.session.QueryPagesWithContext(ctcx, si,
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
func (ddb *dynaPartition) AtomicPut(key string, options ...WriteOption) (bool, *KVPair, error) {
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

	res, err := ddb.session.UpdateItem(&dynamodb.UpdateItemInput{
		TableName:                 aws.String(ddb.GetTableName()),
		Key:                       buildKeys(ddb.partition, key),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	})

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
// FIXME: should the second case just return false, nil?
func (ddb *dynaPartition) AtomicDelete(key string, previous *KVPair) (bool, error) {
	getRes, err := ddb.getKey(key, NewReadOptions())
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

	_, err = ddb.session.DeleteItem(req)
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

func (ddb *dynaPartition) getKey(key string, options *ReadOptions) (*dynamodb.GetItemOutput, error) {
	return ddb.session.GetItem(&dynamodb.GetItemInput{
		TableName:      aws.String(ddb.GetTableName()),
		ConsistentRead: aws.Bool(options.consistent),
		Key: map[string]*dynamodb.AttributeValue{
			"id":   {S: aws.String(ddb.partition)},
			"name": {S: aws.String(key)},
		},
	})
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

// DecodeItem decode a DDB attribute value into a KVPair
func DecodeItem(item map[string]*dynamodb.AttributeValue) (*KVPair, error) {
	kv := &KVPair{}

	err := dynamodbattribute.UnmarshalMap(item, kv)
	if err != nil {
		return nil, err
	}

	if val, ok := item["payload"]; ok {
		kv.value = val
	}

	kv.fields = make(map[string]*dynamodb.AttributeValue)

	for k, v := range item {
		if !isReservedField(k) {
			kv.fields[k] = v
		}
	}

	return kv, nil
}

func isItemExpired(item map[string]*dynamodb.AttributeValue) bool {
	var ttl int64

	if v, ok := item["expires"]; ok {
		ttl, _ = strconv.ParseInt(aws.StringValue(v.N), 10, 64)
		return time.Unix(ttl, 0).Before(time.Now())
	}

	return false
}

// MarshalStruct this helper method marshals a struct into an *dynamodb.AttributeValue which contains a map
// in the format required to provide to WriteWithAttributeValue.
func MarshalStruct(in interface{}) (*dynamodb.AttributeValue, error) {
	item, err := dynamodbattribute.MarshalMap(in)
	if err != nil {
		return nil, err
	}

	return &dynamodb.AttributeValue{M: item}, nil
}

// UnmarshalStruct this helper method un-marshals a struct from an *dynamodb.AttributeValue returned by KVPair.AttributeValue.
func UnmarshalStruct(val *dynamodb.AttributeValue, out interface{}) error {
	return dynamodbattribute.UnmarshalMap(val.M, out)
}
