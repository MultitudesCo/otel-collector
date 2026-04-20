package multitudesauthextension

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/extension/auth"
)

const Type = "multitudes_auth"

func NewFactory() extension.Factory {
	return extension.NewFactory(
		component.MustNewType(Type),
		createDefaultConfig,
		createExtension,
		component.StabilityLevelAlpha,
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createExtension(
	_ context.Context,
	set extension.Settings,
	cfg component.Config,
) (extension.Extension, error) {
	_ = cfg.(*Config)
	ext := newExtension(set.Logger)
	return auth.NewServer(
		auth.WithServerAuthenticate(ext.Authenticate),
		auth.WithServerStart(ext.Start),
		auth.WithServerShutdown(ext.Shutdown),
	), nil
}
