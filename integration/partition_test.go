package integration

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"github.com/wolfeidau/dynastore"
)

type indexFields struct {
	Created string `json:"created"`
}

type globalIndexFields struct {
	Username string `json:"pk1"`
	Created  string `json:"sk1"`
}

func Test(t *testing.T) {
	assert := require.New(t)

	err := ensureVersionTable(dbSvc, "testing-locks")
	assert.NoError(err)

	dl := dynastore.NewWithClient(dbSvc, &dynastore.StoreHooks{
		RequestBuilt: func(ctx context.Context, params interface{}) context.Context {
			log.Info().Fields(map[string]interface{}{
				"operation": dynastore.OperationName(ctx),
				"params":    params,
			}).Msg("RequestSent")
			return ctx
		},
	})

	testPutGetDeleteExists(t, dl)
	testList(t, dl)
	testListPage(t, dl)
	testAtomicPut(t, dl)
	testAtomicPutLocalIndex(t, dl)
	testAtomicPutGlobalIndex(t, dl)
	testAtomicDelete(t, dl)
}

func ensureVersionTable(dbSvc dynamodbiface.DynamoDBAPI, tableName string) error {
	_, err := dbSvc.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []*dynamodb.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: aws.String(dynamodb.KeyTypeHash)},
			{AttributeName: aws.String("name"), KeyType: aws.String(dynamodb.KeyTypeRange)},
		},
		LocalSecondaryIndexes: []*dynamodb.LocalSecondaryIndex{
			{
				IndexName: aws.String("idx_created"),
				KeySchema: []*dynamodb.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: aws.String(dynamodb.KeyTypeHash)},
					{AttributeName: aws.String("created"), KeyType: aws.String(dynamodb.KeyTypeRange)},
				},
				Projection: &dynamodb.Projection{ProjectionType: aws.String(dynamodb.ProjectionTypeAll)},
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				IndexName: aws.String("idx_global_1"),
				KeySchema: []*dynamodb.KeySchemaElement{
					{AttributeName: aws.String("pk1"), KeyType: aws.String(dynamodb.KeyTypeHash)},
					{AttributeName: aws.String("sk1"), KeyType: aws.String(dynamodb.KeyTypeRange)},
				},
				Projection: &dynamodb.Projection{ProjectionType: aws.String(dynamodb.ProjectionTypeAll)},
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
			{AttributeName: aws.String("name"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
			{AttributeName: aws.String("created"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
			{AttributeName: aws.String("pk1"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
			{AttributeName: aws.String("sk1"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		SSESpecification: &dynamodb.SSESpecification{
			Enabled: aws.Bool(true),
			SSEType: aws.String(dynamodb.SSETypeAes256),
		},
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeResourceInUseException {
				return nil
			}
		}
		return err
	}

	err = dbSvc.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return err
	}

	_, err = dbSvc.UpdateTimeToLive(&dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(tableName),
		TimeToLiveSpecification: &dynamodb.TimeToLiveSpecification{
			AttributeName: aws.String("expires"),
			Enabled:       aws.Bool(true),
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func testPutGetDeleteExists(t *testing.T, dSession *dynastore.DynaSession) {
	assert := require.New(t)

	// , tableName: "testing-locks", partition: "agent"
	kv := dSession.Table("testing-locks").Partition("agent")

	// Get a not exist key should return ErrKeyNotFound
	_, err := kv.Get("testPutGetDelete_not_exist_key")
	assert.Equal(dynastore.ErrKeyNotFound, err)

	data, err := os.ReadFile("fixtures/pr.json")
	assert.NoError(err)

	value := string(data)

	for _, key := range []string{
		"testPutGetDeleteExists",
		"testPutGetDeleteExists/",
		"testPutGetDeleteExists/testbar/",
		"testPutGetDeleteExists/testbar/testfoobar",
	} {
		t.Run(key, func(t *testing.T) {
			// Put the key
			err = kv.Put(key, dynastore.WriteWithString(value), dynastore.WriteWithTTL(2*time.Second))
			assert.NoError(err)

			var pair *dynastore.KVPair

			// Get should return the value and an incremented index
			pair, err = kv.Get(key)
			assert.NoError(err)
			assert.NotNil(pair)
			assert.Equal(value, pair.StringValue())
			assert.NotEqual(0, pair.Expires)

			assert.NotEqual(0, pair.Version)

			var exists bool

			// Exists should return true
			exists, err = kv.Exists(key)
			assert.NoError(err)
			assert.True(exists)

			// Delete the key
			err = kv.Delete(key)
			assert.NoError(err)

			// Get should fail
			pair, err = kv.Get(key)
			assert.Error(err)
			assert.Nil(pair)
			assert.Nil(pair)

			// Exists should return false
			exists, err = kv.Exists(key)
			assert.NoError(err)
			assert.False(exists)
		})
	}

	key := "something/withoutExpires"

	// Put the key
	err = kv.Put(key, dynastore.WriteWithString(value), dynastore.WriteWithNoExpires(), dynastore.WriteWithNoExpires())
	assert.NoError(err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key)
	assert.NoError(err)
	assert.NotNil(pair)
	assert.Equal(value, pair.StringValue())
	assert.Equal(int64(0), pair.Expires)
}

func testAtomicPut(t *testing.T, dSession *dynastore.DynaSession) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks").Partition("agent")

	t.Run("AtomicPut", func(t *testing.T) {
		key := "testAtomicPut"
		value := []byte("world")

		// Put the key
		err := kv.Put(key, dynastore.WriteWithBytes(value))
		assert.NoError(err)

		// Get should return the value and an incremented index
		pair, err := kv.Get(key)
		assert.NoError(err)
		assert.NotNil(pair)
		assert.Equal(value, pair.BytesValue())
		assert.NotEqual(0, pair.Version)

		// This CAS should fail: previous exists.
		success, _, err := kv.AtomicPut(key, dynastore.WriteWithString("WORLD"))
		assert.Error(err)
		assert.False(success)

		// This CAS should succeed
		success, _, err = kv.AtomicPut(key, dynastore.WriteWithPreviousKV(pair), dynastore.WriteWithBytes([]byte("WORLD")))
		assert.NoError(err)
		assert.True(success)

		// This CAS should fail, key has wrong index.
		pair.Version = 6744
		success, _, err = kv.AtomicPut(key, dynastore.WriteWithPreviousKV(pair), dynastore.WriteWithBytes([]byte("WORLDWORLD")))
		assert.Equal(err, dynastore.ErrKeyModified)
		assert.False(success)
	})
}

func testAtomicPutLocalIndex(t *testing.T, dSession *dynastore.DynaSession) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks").Partition("agent")

	t.Run("AtomicPutLocalIndex", func(t *testing.T) {
		key := "testAtomicPutLocalIndex"
		value := []byte("world")

		timeStamp := "20200103T1100Z"

		// Put the key
		err := kv.Put(key, dynastore.WriteWithBytes(value), dynastore.WriteWithFields(map[string]string{
			"created": timeStamp,
		}))
		assert.NoError(err)

		// Get should return the value and an incremented index
		page, err := kv.ListPage(timeStamp, dynastore.ReadWithLocalIndex("idx_created", "created"))
		assert.NoError(err)
		assert.Equal(1, len(page.Keys))

		idxFields := new(indexFields)
		err = page.Keys[0].DecodeFields(idxFields)
		assert.NoError(err)
		assert.Equal(timeStamp, idxFields.Created)
	})
}

func testAtomicPutGlobalIndex(t *testing.T, dSession *dynastore.DynaSession) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks")

	t.Run("AtomicPutGlobalIndex", func(t *testing.T) {
		sortKey := "testAtomicPutGlobalIndex"
		value := []byte("world")

		username := "wolfeidau"
		timeStamp := "20200103T1100Z"

		// Put the key
		err := kv.PutWithContext(context.TODO(), "agent", sortKey, dynastore.WriteWithBytes(value), dynastore.WriteWithFields(map[string]string{
			"pk1": username,
			"sk1": timeStamp,
		}))
		assert.NoError(err)

		// Get should return the value and an incremented index
		page, err := kv.ListPageWithContext(context.TODO(), username, timeStamp, dynastore.ReadWithGlobalIndex("idx_global_1", "pk1", "sk1"))
		assert.NoError(err)
		assert.Equal(1, len(page.Keys))

		idxFields := new(globalIndexFields)
		err = page.Keys[0].DecodeFields(idxFields)
		assert.NoError(err)
		assert.Equal(username, idxFields.Username)
		assert.Equal(timeStamp, idxFields.Created)
	})
}

func testAtomicDelete(t *testing.T, dSession *dynastore.DynaSession) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks").Partition("agent")

	t.Run("AtomicDelete", func(t *testing.T) {
		key := "testAtomicDelete"
		value := []byte("world")

		// Put the key
		err := kv.Put(key, dynastore.WriteWithBytes(value))
		assert.NoError(err)

		// Get should return the value and an incremented index
		pair, err := kv.Get(key)
		assert.NoError(err)
		assert.NotNil(pair)
		assert.Equal(value, pair.BytesValue())
		assert.NotEqual(0, pair.Version)

		tempIndex := pair.Version

		// AtomicDelete should fail
		pair.Version = 6744
		success, err := kv.AtomicDelete(key, pair)
		assert.Error(err)
		assert.False(success)

		// AtomicDelete should succeed
		pair.Version = tempIndex
		success, err = kv.AtomicDelete(key, pair)
		assert.NoError(err)
		assert.True(success)

		// Delete a non-existent key; should fail
		success, err = kv.AtomicDelete(key, pair)
		assert.Equal(dynastore.ErrKeyNotFound, err)
		assert.False(success)
	})
}

func testList(t *testing.T, dSession *dynastore.DynaSession) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks").Partition("agent")

	t.Run("List", func(t *testing.T) {
		childKey := "testList/child"
		subfolderKey := "testList/subfolder"

		// Put the first child key
		err := kv.Put(childKey, dynastore.WriteWithBytes([]byte("first")))
		assert.NoError(err)

		// Put the second child key which is also a directory
		err = kv.Put(subfolderKey, dynastore.WriteWithBytes([]byte("second")))
		assert.NoError(err)

		// Put child keys under secondKey
		for i := 1; i <= 3; i++ {
			key := "testList/subfolder/key" + strconv.Itoa(i)
			err = kv.Put(key, dynastore.WriteWithBytes([]byte("value")))
			assert.NoError(err)
		}

		// List should work and return five child entries
		pairs, err := kv.List("testList/subfolder/key")
		assert.NoError(err)
		assert.NotNil(pairs)
		assert.Equal(3, len(pairs))
	})
}

func testListPage(t *testing.T, dSession *dynastore.DynaSession) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks").Partition("agent")

	t.Run("ListPage", func(t *testing.T) {
		childKey := "testList/child"
		subfolderKey := "testList/subfolder"

		// Put the first child key
		err := kv.Put(childKey, dynastore.WriteWithBytes([]byte("first")))
		assert.NoError(err)

		// Put the second child key which is also a directory
		err = kv.Put(subfolderKey, dynastore.WriteWithBytes([]byte("second")))
		assert.NoError(err)

		// Put child keys under secondKey
		for i := 1; i <= 3; i++ {
			key := "testList/subfolder/key" + strconv.Itoa(i)
			err = kv.Put(key, dynastore.WriteWithBytes([]byte("value")))
			assert.NoError(err)
		}

		// List should work and return child entries
		page, err := kv.ListPage("testList/subfolder/key")
		assert.NoError(err)
		assert.NotNil(page)
		assert.Equal(3, len(page.Keys))

		// List should work and return all child entries
		page, err = kv.ListPage("")
		assert.NoError(err)
		assert.NotNil(page)
		assert.Equal(6, len(page.Keys))

		// List should work and return all child entries
		page, err = kv.ListPage("", dynastore.ReadWithLimit(5), dynastore.ReadScanIndexForwardDisable())
		assert.NoError(err)
		assert.NotNil(page)
		assert.Equal(5, len(page.Keys))
		assert.Equal("testList/child", page.Keys[4].Key)

		page, err = kv.ListPage("", dynastore.ReadWithStartKey(page.LastKey), dynastore.ReadScanIndexForwardDisable())
		assert.NoError(err)
		assert.NotNil(page)
		assert.Equal(1, len(page.Keys))
	})
}
