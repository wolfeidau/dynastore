package dynastore

import (
	"context"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// DecodeItem decode a DDB attribute value into a KVPair
func DecodeItem(item map[string]*dynamodb.AttributeValue) (*KVPair, error) {
	kv := new(KVPair)

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

func isReservedField(s string) bool {
	_, ok := reservedFields[s]
	return ok
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

// OperationName extracts the name of the operation being handled in the given
// context. If it is not known, it returns ("").
func OperationName(ctx context.Context) string {
	name, _ := ctx.Value(OperationNameKey).(string)
	return name
}

func setOperationName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, OperationNameKey, name)
}
