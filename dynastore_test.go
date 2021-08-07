package dynastore

import (
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/dhui/dktest"
	"github.com/stretchr/testify/require"
)

const (
	defaultRegion = "us-east-1"
)

var (
	opts = dktest.Options{PortRequired: true, ReadyFunc: isReady}
)

type indexFields struct {
	Created string `json:"created"`
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	dbSvc := dynamodb.New(mustSession(c.FirstPort()))

	_, err := dbSvc.ListTablesWithContext(ctx, &dynamodb.ListTablesInput{})

	return err == nil
}

func Test(t *testing.T) {
	dktest.Run(t, "amazon/dynamodb-local:latest", opts,
		func(t *testing.T, c dktest.ContainerInfo) {
			assert := require.New(t)

			dbSvc := dynamodb.New(mustSession(c.FirstPort()))

			err := ensureVersionTable(dbSvc, "testing-locks")
			assert.NoError(err)

			dl := &dynaSession{DynamoDBAPI: dbSvc}

			testPutGetDeleteExists(t, dl)
			testList(t, dl)
			testListPage(t, dl)
			testAtomicPut(t, dl)
			testAtomicPutIndex(t, dl)
			testAtomicDelete(t, dl)
		})
}

func mustSession(hostIP, hostPort string, err error) *session.Session {
	if err != nil {
		panic(err)
	}

	ddbURL := fmt.Sprintf("http://%s:%s", hostIP, hostPort)

	creds := credentials.NewStaticCredentials("123", "test", "test")
	return session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(defaultRegion),
		Endpoint:    aws.String(ddbURL),
		Credentials: creds,
	}))
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
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
			{AttributeName: aws.String("name"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
			{AttributeName: aws.String("created"), AttributeType: aws.String(dynamodb.ScalarAttributeTypeS)},
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

func testPutGetDeleteExists(t *testing.T, dSession Session) {
	assert := require.New(t)

	// , tableName: "testing-locks", partition: "agent"
	kv := dSession.Table("testing-locks").Partition("agent")

	// Get a not exist key should return ErrKeyNotFound
	_, err := kv.Get("testPutGetDelete_not_exist_key")
	assert.Equal(ErrKeyNotFound, err)

	data, err := ioutil.ReadFile("fixtures/pr.json")
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
			err = kv.Put(key, WriteWithString(value), WriteWithTTL(2*time.Second))
			assert.NoError(err)

			var pair *KVPair

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
	err = kv.Put(key, WriteWithString(value), WriteWithNoExpires(), WriteWithNoExpires())
	assert.NoError(err)

	// Get should return the value and an incremented index
	pair, err := kv.Get(key)
	assert.NoError(err)
	assert.NotNil(pair)
	assert.Equal(value, pair.StringValue())
	assert.Equal(int64(0), pair.Expires)
}

func testAtomicPut(t *testing.T, dSession Session) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks").Partition("agent")

	t.Run("AtomicPut", func(t *testing.T) {
		key := "testAtomicPut"
		value := []byte("world")

		// Put the key
		err := kv.Put(key, WriteWithBytes(value))
		assert.NoError(err)

		// Get should return the value and an incremented index
		pair, err := kv.Get(key)
		assert.NoError(err)
		assert.NotNil(pair)
		assert.Equal(value, pair.BytesValue())
		assert.NotEqual(0, pair.Version)

		// This CAS should fail: previous exists.
		success, _, err := kv.AtomicPut(key, WriteWithString("WORLD"))
		assert.Error(err)
		assert.False(success)

		// This CAS should succeed
		success, _, err = kv.AtomicPut(key, WriteWithPreviousKV(pair), WriteWithBytes([]byte("WORLD")))
		assert.NoError(err)
		assert.True(success)

		// This CAS should fail, key has wrong index.
		pair.Version = 6744
		success, _, err = kv.AtomicPut(key, WriteWithPreviousKV(pair), WriteWithBytes([]byte("WORLDWORLD")))
		assert.Equal(err, ErrKeyModified)
		assert.False(success)
	})
}

func testAtomicPutIndex(t *testing.T, dSession Session) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks").Partition("agent")

	t.Run("AtomicPut", func(t *testing.T) {
		key := "testAtomicPutIndex"
		value := []byte("world")

		timeStamp := "20200103T1100Z"

		// Put the key
		err := kv.Put(key, WriteWithBytes(value), WriteWithFields(map[string]*dynamodb.AttributeValue{
			"created": {S: aws.String(timeStamp)},
		}))
		assert.NoError(err)

		// Get should return the value and an incremented index
		page, err := kv.ListPage(timeStamp, ReadWithLocalIndex("idx_created", "created"))
		assert.NoError(err)
		assert.Equal(1, len(page.Keys))

		idxFields := new(indexFields)
		err = page.Keys[0].DecodeFields(idxFields)
		assert.NoError(err)
		assert.Equal(timeStamp, idxFields.Created)
	})
}

func testAtomicDelete(t *testing.T, dSession Session) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks").Partition("agent")

	t.Run("AtomicDelete", func(t *testing.T) {
		key := "testAtomicDelete"
		value := []byte("world")

		// Put the key
		err := kv.Put(key, WriteWithBytes(value))
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
		assert.Equal(ErrKeyNotFound, err)
		assert.False(success)
	})
}

func testList(t *testing.T, dSession Session) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks").Partition("agent")

	t.Run("List", func(t *testing.T) {
		childKey := "testList/child"
		subfolderKey := "testList/subfolder"

		// Put the first child key
		err := kv.Put(childKey, WriteWithBytes([]byte("first")))
		assert.NoError(err)

		// Put the second child key which is also a directory
		err = kv.Put(subfolderKey, WriteWithBytes([]byte("second")))
		assert.NoError(err)

		// Put child keys under secondKey
		for i := 1; i <= 3; i++ {
			key := "testList/subfolder/key" + strconv.Itoa(i)
			err = kv.Put(key, WriteWithBytes([]byte("value")))
			assert.NoError(err)
		}

		// List should work and return five child entries
		pairs, err := kv.List("testList/subfolder/key")
		assert.NoError(err)
		assert.NotNil(pairs)
		assert.Equal(3, len(pairs))
	})
}

func testListPage(t *testing.T, dSession Session) {
	assert := require.New(t)

	kv := dSession.Table("testing-locks").Partition("agent")

	t.Run("ListPage", func(t *testing.T) {
		childKey := "testList/child"
		subfolderKey := "testList/subfolder"

		// Put the first child key
		err := kv.Put(childKey, WriteWithBytes([]byte("first")))
		assert.NoError(err)

		// Put the second child key which is also a directory
		err = kv.Put(subfolderKey, WriteWithBytes([]byte("second")))
		assert.NoError(err)

		// Put child keys under secondKey
		for i := 1; i <= 3; i++ {
			key := "testList/subfolder/key" + strconv.Itoa(i)
			err = kv.Put(key, WriteWithBytes([]byte("value")))
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
		page, err = kv.ListPage("", ReadWithLimit(5))
		assert.NoError(err)
		assert.NotNil(page)
		assert.Equal(5, len(page.Keys))

		page, err = kv.ListPage("", ReadWithStartKey(page.LastKey))
		assert.NoError(err)
		assert.NotNil(page)
		assert.Equal(1, len(page.Keys))
	})
}
