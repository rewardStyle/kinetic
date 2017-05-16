package listener

// KclReaderConfig is used to configure KclReader
type KclReaderConfig struct {
	*kclReaderOptions
}

// NewKclReaderConfig creates a new instance of KclReaderConfig
func NewKclReaderConfig() *KclReaderConfig {
	return &KclReaderConfig{
		kclReaderOptions: &kclReaderOptions{},
	}
}

// SetOnInitCallbackFn configures a callback function which is run prior to sending a status message
// acknowledging an 'initialize' message was received / processed
func (c *KclReaderConfig) SetOnInitCallbackFn(fn func() error) {
	c.onInitCallbackFn = fn
}

// SetOnCheckpointCallbackFn configures a callback function which is run prior to sending a status message
// acknowledging an 'checkpoint' message was received / processed
func (c *KclReaderConfig) SetOnCheckpointCallbackFn(fn func() error) {
	c.onCheckpointCallbackFn = fn
}

// SetOnShutdownCallbackFn configures a callback function which is run prior to sending a status message
// acknowledging a 'shutdown' message was received / processed
func (c *KclReaderConfig) SetOnShutdownCallbackFn(fn func() error) {
	c.onShutdownCallbackFn = fn
}