package metrics

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdk "go.opentelemetry.io/otel/sdk/metric"
)

var (
	meter = otel.Meter("seidb")

	SeiDBMetrics = struct {
		RestartLatency          metric.Float64Histogram
		SnapshotCreationLatency metric.Float64Histogram
		CommitLatency           metric.Float64Histogram
		MemNodeTotalSize        metric.Float64Gauge
		MemNodeCount            metric.Float64Gauge
	}{
		RestartLatency: must(meter.Float64Histogram(
			"restart_latency",
			metric.WithDescription("Time taken to restart the memiavl database"),
			metric.WithUnit("s"),
		)),
		SnapshotCreationLatency: must(meter.Float64Histogram(
			"snapshot_creation_latency",
			metric.WithDescription("Time taken to create memiavl snapshot"),
			metric.WithUnit("s"),
		)),
		CommitLatency: must(meter.Float64Histogram(
			"commit_latency",
			metric.WithDescription("Time taken to commit"),
			metric.WithUnit("ms"),
		)),
		MemNodeTotalSize: must(meter.Float64Gauge(
			"mem_node_total_size",
			metric.WithDescription("Time taken to restart the memiavl database"),
			metric.WithUnit("s"),
		)),
		MemNodeCount: must(meter.Float64Gauge(
			"mem_node_count",
			metric.WithDescription("Time taken to restart the memiavl database"),
			metric.WithUnit("s"),
		)),
	}
)

// must panics if err is non-nil, otherwise returns v.
func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}

func SetupMetricsProvider(ctx context.Context, listenAddr string) error {
	metricsExporter, err := prometheus.New(prometheus.WithNamespace("seidb"))
	if err != nil {
		return fmt.Errorf("failed to create Prometheus exporter: %w", err)
	}
	otel.SetMeterProvider(sdk.NewMeterProvider(sdk.WithReader(metricsExporter)))
	go func() {
		defer func() { _ = metricsExporter.Shutdown(ctx) }()
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(listenAddr, nil)
		if err != nil {
			log.Printf("failed to serve metrics: %v", err)
			return
		}
	}()
	return nil
}
