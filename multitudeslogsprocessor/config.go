package multitudeslogsprocessor

// Config defines configuration for the Multitudes logs processor.
// The processor requires no configuration — it reads the Bearer token
// from the pipeline context (placed there by the multitudes_auth extension)
// and writes it into each ResourceLogs resource attribute so it survives
// the batch processor.
type Config struct{}

func (cfg *Config) Validate() error {
	return nil
}
