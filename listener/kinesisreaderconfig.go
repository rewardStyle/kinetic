package listener

type KinesisReaderConfig struct {
	*kinesisReaderOptions
}

func NewKinesisReaderConfig(stream, shard string) *KinesisReaderConfig {
	return &KinesisReaderConfig{
		kinesisReaderOptions: &kinesisReaderOptions{
			stream:        stream,
			shard:         shard,
			batchSize:     10000,
			shardIterator: NewShardIterator(),
		},
	}
}

// SetBatchSize configures the batch size of the GetRecords call.
func (c *KinesisReaderConfig) SetBatchSize(batchSize int) {
	c.batchSize = batchSize
}

// SetInitialShardIterator configures the settings used to retrieve initial
// shard iterator via the GetShardIterator call.
func (c *KinesisReaderConfig) SetInitialShardIterator(shardIterator *ShardIterator) {
	c.shardIterator = shardIterator
}
