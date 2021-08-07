package dynastore_test

import (
	"encoding/json"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/wolfeidau/dynastore"
)

type Customer struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

func (c *Customer) ToJSON() string {
	data, _ := json.Marshal(c)
	return string(data)
}

type Index struct {
	Created string `json:"created"`
}

func (cf *Index) ToFields() map[string]string {
	return map[string]string{
		"created": cf.Created,
	}
}

func ExamplePartition_AtomicPut() {
	awsCfg := &aws.Config{}

	client := dynastore.New(awsCfg)

	tbl := client.Table("CRMTable")

	customersPart := tbl.Partition("customers")

	// the payload of the record contains any data which isn't a part of the sort key
	newCustomer := &Customer{Name: "welcome"}

	// a local index which uses created is provided in the extra fields, this is used to list records by creation date
	indexFields := &Index{Created: time.Now().Format(time.RFC3339)}

	// create a record using atomic put, this will ensure the key is unique with any collision triggering a error
	created, kv, err := customersPart.AtomicPut("01FCFSDXQ8EYFCNMEA7C2WJG74",
		dynastore.WriteWithString(newCustomer.ToJSON()), dynastore.WriteWithFields(indexFields.ToFields()))
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
	created, kv, err = customersPart.AtomicPut("01FCFSDXQ8EYFCNMEA7C2WJG74",
		dynastore.WriteWithString(newCustomer.ToJSON()), dynastore.WriteWithPreviousKV(kv))
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
}
