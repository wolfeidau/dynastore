package dynastore

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	dexp "github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type contextKey int

const (
	OperationNameKey contextKey = 1 + iota

	listDefaultTimeout = time.Second * 10
)

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
