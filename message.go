package kinetic

import awsKinesis "github.com/aws/aws-sdk-go/service/kinesis"

// Message represents an item on the Kinesis stream
type Message struct {
	awsKinesis.Record
}

// Init initializes a Message
func (k *Message) Init(msg []byte, key string, sequenceNumber *string) *Message {
	return &Message{
		awsKinesis.Record{
			Data:           msg,
			PartitionKey:   &key,
			SequenceNumber: sequenceNumber,
		},
	}
}

// SetValue sets the underlying value of the gokinesis record
func (k *Message) SetValue(value []byte) {
	k.Record.Data = value
}

// Value gets the underlying value of the gokinesis record
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
