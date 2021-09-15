package dynastore

import (
	"encoding/base64"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	indexTypeLocal  = "local"
	indexTypeGlobal = "global"
)

// SessionOption assign various settings to the session options
type SessionOption func(opts *SessionOptions)

// SessionOptions contains optional request parameters
type SessionOptions struct {
	storeHooks *StoreHooks
}

// NewSessionOptions create session options, assign defaults then accept overrides
func NewSessionOptions(opts ...SessionOption) *SessionOptions {
	// assign a place holder value to detect whether to assign the default TTL
	sessionOpts := &SessionOptions{
		storeHooks: defaultHooks,
	}

	for _, opt := range opts {
		opt(sessionOpts)
	}

	return sessionOpts
}

// SessionWithAWSHooks hooks invoked while using this session
func SessionWithAWSHooks(storeHooks *StoreHooks) SessionOption {
	return func(opts *SessionOptions) {
		opts.storeHooks = storeHooks
	}
}

// WriteOption assign various settings to the write options
type WriteOption func(opts *WriteOptions)

// WriteOptions contains optional request parameters
type WriteOptions struct {
	fields   map[string]*dynamodb.AttributeValue
	value    *string
	ttl      *time.Duration
	previous *KVPair // Optional, previous value used to assert if the record has been modified before an atomic update
}

// Append append more options which supports conditional addition
func (wo *WriteOptions) Append(opts ...WriteOption) {
	for _, opt := range opts {
		opt(wo)
	}
}

// NewWriteOptions create write options, assign defaults then accept overrides
func NewWriteOptions(opts ...WriteOption) *WriteOptions {
	// assign a place holder value to detect whether to assign the default TTL
	writeOpts := &WriteOptions{}

	for _, opt := range opts {
		opt(writeOpts)
	}

	return writeOpts
}

// WriteWithTTL time to live (TTL) to the key which is written
func WriteWithTTL(ttl time.Duration) WriteOption {
	return func(opts *WriteOptions) {
		opts.ttl = &ttl
	}
}

// WriteWithNoExpires time to live (TTL) is set not set so it never expires
func WriteWithNoExpires() WriteOption {
	return func(opts *WriteOptions) {
		opts.ttl = nil
	}
}

// WriteWithBytes encode raw data using base64 and assign this value to the key which is written
func WriteWithBytes(val []byte) WriteOption {
	return func(opts *WriteOptions) {
		opts.value = aws.String(base64.StdEncoding.EncodeToString(val))
	}
}

// WriteWithString assign this value to the key which is written
func WriteWithString(val string) WriteOption {
	return func(opts *WriteOptions) {
		opts.value = aws.String(val)
	}
}

// WriteWithFields assign fields to the top level record, this is used to assign attributes used in indexes
func WriteWithFields(fields map[string]string) WriteOption {
	attr := map[string]*dynamodb.AttributeValue{}

	for k, v := range fields {
		attr[k] = &dynamodb.AttributeValue{S: aws.String(v)}
	}

	return func(opts *WriteOptions) {
		opts.fields = attr
	}
}

// WriteWithPreviousKV previous KV which will be checked prior to update
func WriteWithPreviousKV(previous *KVPair) WriteOption {
	return func(opts *WriteOptions) {
		opts.previous = previous
	}
}

// ReadOption assign various settings to the read options
type ReadOption func(opts *ReadOptions)

type index struct {
	indexType             string
	name                  string
	partitionKeyAttribute string
	sortKeyAttribute      string
}

// ReadOptions contains optional request parameters
type ReadOptions struct {
	consistent       bool
	scanIndexForward bool
	limit            *int64
	startKey         *string
	index            *index
}

// Append append more options which supports conditional addition
func (ro *ReadOptions) Append(opts ...ReadOption) {
	for _, opt := range opts {
		opt(ro)
	}
}

func (ro *ReadOptions) hasIndex() bool {
	return ro.index != nil
}

// NewReadOptions create read options, assign defaults then accept overrides
// enable the read consistent flag by default
func NewReadOptions(opts ...ReadOption) *ReadOptions {
	readOpts := &ReadOptions{
		consistent:       false,
		scanIndexForward: true, // stick with the dynamodb default which is true
	}

	for _, opt := range opts {
		opt(readOpts)
	}

	return readOpts
}

// ReadConsistentDisable disable consistent reads
func ReadConsistentDisable() ReadOption {
	return func(opts *ReadOptions) {
		opts.consistent = false
	}
}

// ReadScanIndexForwardDisable if this is disabled DynamoDB reads the results in reverse order
// by sort key value (DESCENDING ORDER)
func ReadScanIndexForwardDisable() ReadOption {
	return func(opts *ReadOptions) {
		opts.scanIndexForward = false
	}
}

// ReadWithStartKey read a list of records with the exclusive start key provided
// this will apply to list operations only.
func ReadWithStartKey(key string) ReadOption {
	return func(opts *ReadOptions) {
		opts.startKey = aws.String(key)
	}
}

// ReadWithLimit read a list of records with the limit provided
// this will apply to list operations only.
func ReadWithLimit(limit int64) ReadOption {
	return func(opts *ReadOptions) {
		opts.limit = aws.Int64(limit)
	}
}

// ReadWithLocalIndex preform a read using a local index with the given name
// and the name of the sort key attribute.
func ReadWithLocalIndex(name, sortKeyAttribute string) ReadOption {
	return func(opts *ReadOptions) {
		opts.index = &index{
			indexType:        indexTypeLocal,
			name:             name,
			sortKeyAttribute: sortKeyAttribute,
		}
	}
}

// ReadWithGlobalIndex preform a read using a local index with the given name
// and the name of the partition and sort key attributes.
func ReadWithGlobalIndex(name, partitionKeyAttribute, sortKeyAttribute string) ReadOption {
	return func(opts *ReadOptions) {
		opts.index = &index{
			indexType:             indexTypeGlobal,
			name:                  name,
			partitionKeyAttribute: partitionKeyAttribute,
			sortKeyAttribute:      sortKeyAttribute,
		}
	}
}
