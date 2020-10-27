// dynastore offers a simple storage abstraction for AWS DynamoDB
//
// This library encourages developers to build services which store a number of closely related
// entities in the same table. For more information on this pattern
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-modeling-nosql-B.html.
//
// This works by creating a DynamoDB table and using the hash key as the partition name, then storing the entity
// identifier in the range key. This entity identifier can be prefixed with a path to enable storing of hierarchies
// like a filesystem.
//
// To setup a session, configure a table / partition and retrieve a record.
//
//     session := dynastore.New()
//     kv := session.Table("agents").Partition("users")
//
//     key := "user/123"
//     _, err := kv.Get("user/123")
//     if err != nil {
//         if err == dynastore.ErrKeyNotFound {
//             log.Printf("not found: %s", key)
//         }
//     }
//
package dynastore
