package message

import kinesis "github.com/aws/aws-sdk-go/service/kinesis"

// Message represents an item on the Kinesis stream
type Message struct {
	kinesis.Record
}

// Init initializes a Message.
// Currently we are ignoring sequenceNumber.
func (k *Message) Init(msg []byte, key string) *Message {
	return &Message{
		kinesis.Record{
			Data:         msg,
			PartitionKey: &key,
		},
	}
}

// SetValue sets the underlying value of the underlying record
func (k *Message) SetValue(value []byte) {
	k.Record.Data = value
}

// Value gets the underlying value of the underlying record
func (k *Message) Value() []byte {
	return k.Record.Data
}

// Key gets the partion key of the message
func (k *Message) Key() []byte {
	if k.Record.PartitionKey != nil {
		return []byte(*k.PartitionKey)
	}
	return nil
}
