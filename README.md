# dynastore 

This is a small K/V library written Go, which uses [AWS DynamoDB](https://aws.amazon.com/dynamodb/) as the data store.

It supports create, read, update and delete (CRUD) for key/value pairs.

[![GitHub Actions status](https://github.com/wolfeidau/dynastore/workflows/Go/badge.svg?branch=master)](https://github.com/wolfeidau/dynastore/actions?query=workflow%3AGo)
[![Go Report Card](https://goreportcard.com/badge/github.com/wolfeidau/dynastore)](https://goreportcard.com/report/github.com/wolfeidau/dynastore)
[![Documentation](https://godoc.org/github.com/wolfeidau/dynastore?status.svg)](https://godoc.org/github.com/wolfeidau/dynastore) [![Coverage Status](https://coveralls.io/repos/github/wolfeidau/dynastore/badge.svg?branch=master)](https://coveralls.io/github/wolfeidau/dynastore?branch=master)

# Usage

The following example illustrates CRUD with optimistic locking for create, update and delete. To ensure changes are atomic a version attribute stored with the record in dynamodb.

```go
	awsCfg := &aws.Config{}

	client := dynastore.New(awsCfg)

	tbl := client.Table("CRMTable")

	customersPart := tbl.Partition("customers")

	// the payload of the record contains any data which isn't a part of the sort key
	newCustomer := &Customer{Name: "welcome"}

	// a local index which uses created is provided in the extra fields, this is used to list records by creation date
	indexFields := &Index{Created: time.Now().Format(time.RFC3339)}

	// create a record using atomic put, this will ensure the key is unique with any collision triggering a error
	created, kv, err := customersPart.AtomicPut("01FCFSDXQ8EYFCNMEA7C2WJG74", dynastore.WriteWithString(newCustomer.ToJson()), dynastore.WriteWithFields(indexFields.ToFields()))
	if err != nil {
		log.Fatalf("failed to put: %s", err)
	}

	log.Printf("created: %v, id: %s, name: %s, version: %d", created, kv.Partition, kv.Key, kv.Version)

	// read back the records
	page, err := customersPart.ListPage("", dynastore.ReadWithLocalIndex("idx_created", "created"), dynastore.ReadWithLimit(100))
	if err != nil {
		log.Fatalf("failed to put: %s", err)
	}

	log.Printf("found records count: %d", len(page.Keys))

	// update the record
	newCustomer.Status = "enabled"

	// perfom an atomic update, ensuring that the record hasn't changed version in the time between create and update
	// this uses optimistic locking via a version attribute stored with the record in dynamodb
	created, kv, err = customersPart.AtomicPut("01FCFSDXQ8EYFCNMEA7C2WJG74", dynastore.WriteWithString(newCustomer.ToJson()), dynastore.WriteWithPreviousKV(kv))
	if err != nil {
		log.Fatalf("failed to put: %s", err)
	}

	log.Printf("created: %v, id: %s, name: %s version: %d", created, kv.Partition, kv.Key, kv.Version)

	// perform an atomic delete of the record, this again uses optimistic locking via a version attribute stored with the record in dynamodb
	deleted, err := customersPart.AtomicDelete("01FCFSDXQ8EYFCNMEA7C2WJG74", kv)
	if err != nil {
		log.Fatalf("failed to put: %s", err)
	}

	log.Printf("deleted: %v, id: %s, name: %s version: %d", deleted, kv.Partition, kv.Key, kv.Version)

```

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

# References

Prior work in this space:

* https://github.com/wolfeidau/dynalock
* https://github.com/awslabs/dynamodb-lock-client
* https://github.com/intercom/lease

This borrows a lot of ideas, tests and a subset of the API from https://github.com/abronan/valkeyrie.

Updates to the original API are based on a great blog post by @davecheney https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis

# License

This code was authored by [Mark Wolfe](https://github.com/wolfeidau) and licensed under the [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).