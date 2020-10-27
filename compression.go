package dynastore

import (
	"bytes"
	"compress/gzip"
	"encoding/json"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/mr-tron/base58"
)

func compressAndEncodeKey(key map[string]*dynamodb.AttributeValue) (string, error) {
	buf := new(bytes.Buffer)

	wr := gzip.NewWriter(buf)

	err := json.NewEncoder(wr).Encode(key)
	if err != nil {
		return "", err
	}

	err = wr.Flush()
	if err != nil {
		return "", err
	}
	return base58.Encode(buf.Bytes()), nil
}

func decompressAndDecodeKey(key string) (map[string]*dynamodb.AttributeValue, error) {
	data, err := base58.Decode(key)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(data)

	r, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}

	m := make(map[string]*dynamodb.AttributeValue)

	err = json.NewDecoder(r).Decode(&m)
	if err != nil {
		return nil, err
	}

	return m, err
}
