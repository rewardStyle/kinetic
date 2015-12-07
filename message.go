package kinetic

import (
	kinesis "github.com/sendgridlabs/go-kinesis"
)

type Message struct {
	*kinesis.GetRecordsRecords
}

func (k *Message) Init(msg []byte, key string) *Message {
	return &Message{
		&kinesis.GetRecordsRecords{
			Data:         msg,
			PartitionKey: key,
		},
	}
}

func (k *Message) Value() []byte {
	return k.GetData()
}

func (k *Message) Key() []byte {
	return []byte(k.PartitionKey)
}
