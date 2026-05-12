module github.com/multitudes/otel-collector/multitudesexporter

go 1.26

require (
	github.com/multitudes/otel-collector/multitudesauthextension v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/collector/component v0.115.0
	go.opentelemetry.io/collector/consumer v1.21.0
	go.opentelemetry.io/collector/exporter v0.115.0
	go.opentelemetry.io/collector/pdata v1.58.0
	go.uber.org/zap v1.27.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/hashicorp/go-version v1.9.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	go.opentelemetry.io/collector/config/configtelemetry v0.115.0 // indirect
	go.opentelemetry.io/collector/extension v0.115.0 // indirect
	go.opentelemetry.io/collector/extension/auth v0.115.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.58.0 // indirect
	go.opentelemetry.io/collector/pipeline v0.115.0 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/grpc v1.81.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/multitudes/otel-collector/multitudesauthextension => ../multitudesauthextension
