package dynastore

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func Test_compressAndEncodeKey(t *testing.T) {
	type args struct {
		key map[string]*dynamodb.AttributeValue
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "should encode key",
			args: args{key: map[string]*dynamodb.AttributeValue{
				"id": {S: aws.String("welcome")},
			}},
			want: "43hq9ZEtH5MmQ4HqcunoWHkJBoQUnu22Dsa1L9xdfG7ReiUJqPULC8AqoQxYg3jswJH4gGSncjrachbipBHxxmvTAryjUCj2sTomdzs8",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compressAndEncodeKey(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("compressAndEncodeKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("compressAndEncodeKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_decompressAndDecodeKey(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]*dynamodb.AttributeValue
		wantErr bool
	}{
		{
			name: "should decode key",
			args: args{key: "43hq9ZEtH5MmQ4HqcunoWHkJBoQUnu22Dsa1L9xdfG7ReiUJqPULC8AqoQxYg3jswJH4gGSncjrachbipBHxxmvTAryjUCj2sTomdzs8"},
			want: map[string]*dynamodb.AttributeValue{
				"id": {S: aws.String("welcome")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decompressAndDecodeKey(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("decompressAndDecodeKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decompressAndDecodeKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}
