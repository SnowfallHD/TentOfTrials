package analytics

import (
	"strings"
	"testing"
	"time"
)

func sampleWithTags(name string, tags ...MetricTag) MetricSample {
	return MetricSample{
		Name:      name,
		Type:      MetricTypeGauge,
		Value:     1,
		Timestamp: time.Unix(0, 0),
		Tags:      tags,
	}
}

func TestRecordAcceptsSamplesWithinDefaultTagCardinality(t *testing.T) {
	collector := NewCollector()

	if !collector.Record(sampleWithTags("orders.open", MetricTag{Key: "region", Value: "us-east"})) {
		t.Fatal("expected first sample to be accepted")
	}

	stats := collector.Stats()
	if stats.BufferedSamples != 1 {
		t.Fatalf("buffered samples = %d, want 1", stats.BufferedSamples)
	}
	if stats.Dropped != 0 {
		t.Fatalf("dropped = %d, want 0", stats.Dropped)
	}
	if stats.TagCardinalityLimit != DefaultMaxTagCardinality {
		t.Fatalf("tag cardinality limit = %d, want %d", stats.TagCardinalityLimit, DefaultMaxTagCardinality)
	}
	if got := stats.UniqueTagSets["orders.open"]; got != 1 {
		t.Fatalf("unique tag sets = %d, want 1", got)
	}
}

func TestRecordRejectsNewTagSetOverCardinalityLimit(t *testing.T) {
	collector := NewCollector().WithMaxTagCardinality(1)

	if !collector.Record(sampleWithTags("orders.open", MetricTag{Key: "region", Value: "us-east"})) {
		t.Fatal("expected first tag set to be accepted")
	}
	if collector.Record(sampleWithTags("orders.open", MetricTag{Key: "region", Value: "eu-west"})) {
		t.Fatal("expected second unique tag set to be rejected")
	}
	if !collector.Record(sampleWithTags("orders.open", MetricTag{Key: "region", Value: "us-east"})) {
		t.Fatal("expected existing tag set to continue being accepted")
	}

	stats := collector.Stats()
	if stats.BufferedSamples != 2 {
		t.Fatalf("buffered samples = %d, want 2", stats.BufferedSamples)
	}
	if stats.Dropped != 1 {
		t.Fatalf("dropped = %d, want 1", stats.Dropped)
	}
	if !strings.Contains(stats.LastDropReason, DropReasonTagCardinalityLimit) {
		t.Fatalf("last drop reason = %q, want cardinality reason", stats.LastDropReason)
	}
	if got := stats.UniqueTagSets["orders.open"]; got != 1 {
		t.Fatalf("unique tag sets = %d, want 1", got)
	}
}

func TestRecordHonorsCustomTagCardinalityLimit(t *testing.T) {
	collector := NewCollector().WithMaxTagCardinality(2)

	accepted := []MetricSample{
		sampleWithTags("latency", MetricTag{Key: "endpoint", Value: "/quotes"}),
		sampleWithTags("latency", MetricTag{Key: "endpoint", Value: "/orders"}),
	}
	for _, sample := range accepted {
		if !collector.Record(sample) {
			t.Fatalf("expected sample %+v to be accepted", sample.Tags)
		}
	}
	if collector.Record(sampleWithTags("latency", MetricTag{Key: "endpoint", Value: "/fills"})) {
		t.Fatal("expected third unique tag set to be rejected")
	}

	stats := collector.Stats()
	if stats.TagCardinalityLimit != 2 {
		t.Fatalf("tag cardinality limit = %d, want 2", stats.TagCardinalityLimit)
	}
	if got := stats.UniqueTagSets["latency"]; got != 2 {
		t.Fatalf("unique tag sets = %d, want 2", got)
	}
	if stats.BufferedSamples != 2 || stats.Dropped != 1 {
		t.Fatalf("stats = buffered %d dropped %d, want buffered 2 dropped 1", stats.BufferedSamples, stats.Dropped)
	}
}

func TestTagSetSignatureIgnoresTagOrder(t *testing.T) {
	collector := NewCollector().WithMaxTagCardinality(1)

	first := sampleWithTags("quotes", MetricTag{Key: "region", Value: "us"}, MetricTag{Key: "tier", Value: "pro"})
	second := sampleWithTags("quotes", MetricTag{Key: "tier", Value: "pro"}, MetricTag{Key: "region", Value: "us"})
	if !collector.Record(first) {
		t.Fatal("expected first sample to be accepted")
	}
	if !collector.Record(second) {
		t.Fatal("expected same tag set in different order to be accepted")
	}

	stats := collector.Stats()
	if got := stats.UniqueTagSets["quotes"]; got != 1 {
		t.Fatalf("unique tag sets = %d, want 1", got)
	}
}
