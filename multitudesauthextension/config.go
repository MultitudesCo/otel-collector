package multitudesauthextension

// Config defines configuration for the Multitudes auth extension.
// Currently no fields are required — the extension always extracts
// the Bearer token from the incoming Authorization header and forwards
// it through the pipeline.
type Config struct{}

func (cfg *Config) Validate() error {
	return nil
}
