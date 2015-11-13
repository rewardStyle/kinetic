package kinetic

import (
	kinesis "github.com/sendgridlabs/go-kinesis"
)

type KinesisMessage struct {
	*kinesis.GetRecordsRecords
}

func (k *KinesisMessage) Init(msg []byte, key string) *KinesisMessage {
	return &KinesisMessage{
		&kinesis.GetRecordsRecords{
			Data:         msg,
			PartitionKey: key,
		},
	}
}

func (k *KinesisMessage) Value() []byte {
	return k.GetData()
}

func (k *KinesisMessage) Key() []byte {
	return []byte(k.PartitionKey)
}
