package listener

type KclReaderConfig struct {
	*kclReaderOptions
}

func NewKclReaderConfig() *KclReaderConfig {
	return &KclReaderConfig{
		kclReaderOptions: &kclReaderOptions{},
	}
}

func (c *KclReaderConfig) SetOnInitCallbackFn(fn func() error) {
	c.onInitCallbackFn = fn
}

func (c *KclReaderConfig) SetOnCheckpointCallbackFn(fn func() error) {
	c.onCheckpointCallbackFn = fn
}

func (c *KclReaderConfig) SetOnShutdownCallbackFn(fn func() error) {
	c.onShutdownCallbackFn = fn
}