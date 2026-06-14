package observability

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	TraceparentHeader = "traceparent"
	TracestateHeader  = "tracestate"
)

type TraceHeaders struct {
	Traceparent string
	Tracestate  string
}

type MessageHeader struct {
	Key   string
	Value []byte
}

func TraceHeadersFromContext(ctx context.Context) TraceHeaders {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	return TraceHeaders{
		Traceparent: carrier.Get(TraceparentHeader),
		Tracestate:  carrier.Get(TracestateHeader),
	}
}

func ContextFromTraceHeaders(ctx context.Context, headers TraceHeaders) context.Context {
	carrier := propagation.MapCarrier{}
	carrier.Set(TraceparentHeader, headers.Traceparent)
	carrier.Set(TracestateHeader, headers.Tracestate)

	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func ContextFromMessageHeaders(ctx context.Context, headers []MessageHeader) context.Context {
	return ContextFromTraceHeaders(ctx, TraceHeadersFromMessageHeaders(headers))
}

func TraceHeadersFromMessageHeaders(headers []MessageHeader) TraceHeaders {
	var traceHeaders TraceHeaders

	for _, header := range headers {
		switch header.Key {
		case TraceparentHeader:
			traceHeaders.Traceparent = string(header.Value)
		case TracestateHeader:
			traceHeaders.Tracestate = string(header.Value)
		}
	}

	return traceHeaders
}
