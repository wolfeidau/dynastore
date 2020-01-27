# dynalock 

This is a small K/V library written Go, which uses [AWS DynamoDB](https://aws.amazon.com/dynamodb/) as the data store.

It supports create, read, update and delete (CRUD) for key/value pairs.

[![GitHub Actions status](https://github.com/wolfeidau/dynastore/workflows/Go/badge.svg?branch=master)](https://github.com/wolfeidau/dynastore/actions?query=workflow%3AGo)
[![Go Report Card](https://goreportcard.com/badge/github.com/wolfeidau/dynastore)](https://goreportcard.com/report/github.com/wolfeidau/dynastore)
[![Documentation](https://godoc.org/github.com/wolfeidau/dynastore?status.svg)](https://godoc.org/github.com/wolfeidau/dynastore)

# What is the problem?

The main problems I am trying to solve in with this package are:

1. Enable users of the API to store and coordinate work across resources, using multiple lambdas, and containers running in a range of services.
2. Provide a solid and simple storage API which can be used no matter how small your project is.
4. Try and make this API simple, while also reduce the operations for this service using AWS services.

# Why DynamoDB?

DynamoDB is used for storage in a range of Amazon provided APIs and libraries, so I am not the first to do this. see [references](#references). This service also satisfy the requirement to be easy to start with as it is a managed service, no EC2 or patching required.

# Cost?

I am currently working on some testing around this, but with a bit of tuning you can keep the read/write load very low. But this is specifically designed as a starting point, while ensuring there is a clear abstraction between the underlying services and your code. 

To manage this I would recommend you set alarms for read / write metrics, start with on demand but you will probably want to switch to specific read/write limits for production.

# Usage

The main interfaces are as follows, for something more complete see the [competing consumers example](examples/competing-consumers/main.go).

```go
// Store represents the backend K/V storage
type Store interface {
	// Put a value at the specified key
	Put(key string, options ...WriteOption) error

	// Get a value given its key
	Get(key string, options ...ReadOption) (*KVPair, error)

	// List the content of a given prefix
	List(prefix string, options ...ReadOption) ([]*KVPair, error)

	// Delete the value at the specified key
	Delete(key string) error

	// Verify if a Key exists in the store
	Exists(key string, options ...ReadOption) (bool, error)
}
```

# References

Prior work in this space:

* https://github.com/wolfeidau/dynalock
* https://github.com/awslabs/dynamodb-lock-client
* https://github.com/intercom/lease

This borrows a lot of ideas, tests and a subset of the API from https://github.com/abronan/valkeyrie.

Updates to the original API are based on a great blog post by @davecheney https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis

# License

This code was authored by [Mark Wolfe](https://github.com/wolfeidau) and licensed under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).