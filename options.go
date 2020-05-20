package dynastore

import (
	"encoding/base64"
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

// WriteOption assign various settings to the write options
type WriteOption func(opts *WriteOptions)

// WriteOptions contains optional request parameters
type WriteOptions struct {
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

// WriteWithBytes byte slice to the key which is written
func WriteWithBytes(val []byte) WriteOption {
	return func(opts *WriteOptions) {
		opts.value = aws.String(base64.StdEncoding.EncodeToString(val))
	}
}

// WriteWithPreviousKV previous KV which will be checked prior to update
func WriteWithPreviousKV(previous *KVPair) WriteOption {
	return func(opts *WriteOptions) {
		opts.previous = previous
		if opts.previous != nil {
			v := time.Until(time.Unix(opts.previous.Expires, 0)) // update the TTL to the remaining time
			opts.ttl = &v
		}
	}
}

// ReadOption assign various settings to the read options
type ReadOption func(opts *ReadOptions)

// ReadOptions contains optional request parameters
type ReadOptions struct {
	consistent bool
	limit      *int64
	startKey   *string
}

// Append append more options which supports conditional addition
func (ro *ReadOptions) Append(opts ...ReadOption) {
	for _, opt := range opts {
		opt(ro)
	}
}

// NewReadOptions create read options, assign defaults then accept overrides
// enable the read consistent flag by default
func NewReadOptions(opts ...ReadOption) *ReadOptions {

	readOpts := &ReadOptions{
		consistent: true,
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
