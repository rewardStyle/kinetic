package kinetic

import (
	gokinesis "github.com/rewardStyle/go-kinesis"
)

type Message struct {
	*gokinesis.GetRecordsRecords
}

func (k *Message) Init(msg []byte, key string) *Message {
	return &Message{
		&gokinesis.GetRecordsRecords{
			Data:         msg,
			PartitionKey: key,
		},
	}
}

func (k *Message) SetValue(value []byte) {
	k.GetRecordsRecords.Data = value
}

func (k *Message) Value() []byte {
	return k.GetData()
}

func (k *Message) Key() []byte {
	return []byte(k.PartitionKey)
}
