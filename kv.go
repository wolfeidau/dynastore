package dynastore

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
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
