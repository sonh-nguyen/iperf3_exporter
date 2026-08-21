package iperf

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"testing"
	"time"
)

// udpMultiStreamFixture is a trimmed version of the real JSON output from
// `iperf3 -J -c 127.0.0.1 -u -P 4`, captured to verify how iperf3 itself
// aggregates parallel UDP streams (streams[].udp is per-stream, sum is the
// receiver-side aggregate).
const udpMultiStreamFixture = `{
  "start": { "test_start": { "protocol": "UDP" } },
  "end": {
    "streams": [
      { "udp": { "seconds": 2.00026, "bytes": 5013504, "packets": 153, "lost_packets": 0, "jitter_ms": 0.023088277578783588 } },
      { "udp": { "seconds": 2.00026, "bytes": 5013504, "packets": 153, "lost_packets": 0, "jitter_ms": 0.029463306961480844 } },
      { "udp": { "seconds": 2.00026, "bytes": 5013504, "packets": 153, "lost_packets": 0, "jitter_ms": 0.017009802683740156 } },
      { "udp": { "seconds": 2.00026, "bytes": 5013504, "packets": 153, "lost_packets": 0, "jitter_ms": 0.00680089719589579 } }
    ],
    "sum": {
      "seconds": 2.000306, "bytes": 20054016, "bits_per_second": 80205637.26715527,
      "jitter_ms": 0.0190905711049751, "lost_packets": 0, "packets": 612, "lost_percent": 0
    }
  }
}`

func almostEqual(a, b, epsilon float64) bool {
	return math.Abs(a-b) <= epsilon
}

func TestRunUDPMultiStreamAggregation(t *testing.T) {
	restore := mockHelperProcess(t, udpMultiStreamFixture)
	defer restore()

	result := Run(context.Background(), Config{
		Target:  "127.0.0.1",
		Port:    5201,
		Period:  2 * time.Second,
		UDPMode: true,
		Streams: 4,
		Logger:  slog.Default(),
	})

	if !result.Success {
		t.Fatalf("expected success, got error: %v", result.Error)
	}

	// Sent* fields must be aggregated across all 4 streams, not just streams[0].
	if result.SentBytes != 20054016 {
		t.Errorf("SentBytes = %v, want 20054016 (sum of 4 streams)", result.SentBytes)
	}

	if result.SentPackets != 612 {
		t.Errorf("SentPackets = %v, want 612 (sum of 4 streams)", result.SentPackets)
	}

	if result.SentLostPackets != 0 {
		t.Errorf("SentLostPackets = %v, want 0", result.SentLostPackets)
	}

	if result.SentLostPercent != 0 {
		t.Errorf("SentLostPercent = %v, want 0", result.SentLostPercent)
	}

	// Jitter is the mean across streams — verified against iperf3's own
	// end.sum.jitter_ms (0.0190905711049751), which iperf3 computes the same way.
	if !almostEqual(result.SentJitter, 0.0190905711049751, 1e-9) {
		t.Errorf("SentJitter = %v, want ~0.0190905711049751 (mean of 4 streams)", result.SentJitter)
	}

	wantBps := 20054016.0 * 8 / 2.00026
	if !almostEqual(result.SentBitsPerSecond, wantBps, 1) {
		t.Errorf("SentBitsPerSecond = %v, want ~%v", result.SentBitsPerSecond, wantBps)
	}

	// Received* fields come straight from end.sum and must be unaffected by
	// the Sent-side aggregation fix.
	if result.ReceivedBytes != 20054016 {
		t.Errorf("ReceivedBytes = %v, want 20054016", result.ReceivedBytes)
	}

	if result.ReceivedPackets != 612 {
		t.Errorf("ReceivedPackets = %v, want 612", result.ReceivedPackets)
	}
}

// mockHelperProcess redirects execCommandContext to re-invoke this test
// binary as a subprocess that just prints the given fixture to stdout and
// exits 0 — the standard Go "fake exec" pattern for testing code that shells
// out, without depending on a real iperf3 binary being installed.
func mockHelperProcess(t *testing.T, fixture string) (restore func()) {
	t.Helper()

	origExecCommandContext := execCommandContext
	origExecCommand := execCommand

	fakeCmd := func(_ context.Context, _ string, _ ...string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess") //nolint:gosec // re-invokes this test binary only
		cmd.Env = []string{"GO_WANT_HELPER_PROCESS=1", "HELPER_FIXTURE=" + fixture}

		return cmd
	}

	execCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		return fakeCmd(ctx, name, args...)
	}
	execCommand = func(name string, args ...string) *exec.Cmd {
		return fakeCmd(context.Background(), name, args...)
	}

	return func() {
		execCommandContext = origExecCommandContext
		execCommand = origExecCommand
	}
}

// TestHelperProcess is not a real test — it's a subprocess entry point
// spawned by mockHelperProcess. It no-ops under a normal `go test` run.
func TestHelperProcess(_ *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	defer os.Exit(0)

	fmt.Fprint(os.Stdout, os.Getenv("HELPER_FIXTURE"))
}
