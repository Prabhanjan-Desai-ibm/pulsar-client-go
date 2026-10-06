package koddi

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// latencyTracker keeps the last maxSamples publish round-trip durations in a
// circular ring buffer and can compute p50/p99/max/avg on demand.
//
// Hot path (record) is a single mutex lock + one int64 write — negligible overhead.
// Snapshot copies + sorts the buffer once; called at most once per minute.
const maxLatencySamples = 1000

type latencyTracker struct {
	mu      sync.Mutex
	samples [maxLatencySamples]int64 // nanoseconds, circular
	head    int
	count   int64 // total ever recorded
}

// record adds one RTT observation. Overwrites the oldest entry once full.
func (t *latencyTracker) record(d time.Duration) {
	t.mu.Lock()
	t.samples[t.head] = d.Nanoseconds()
	t.head = (t.head + 1) % maxLatencySamples
	t.count++
	t.mu.Unlock()
}

// snapshot computes percentiles. Returns nil if no samples recorded yet.
func (t *latencyTracker) snapshot() *latencySnapshot {
	t.mu.Lock()
	if t.count == 0 {
		t.mu.Unlock()
		return nil
	}
	n := int(t.count)
	if n > maxLatencySamples {
		n = maxLatencySamples
	}
	buf := make([]int64, n)
	copy(buf, t.samples[:n])
	total := t.count
	t.mu.Unlock()

	sort.Slice(buf, func(i, j int) bool { return buf[i] < buf[j] })

	pct := func(p float64) time.Duration {
		idx := int(math.Ceil(p/100.0*float64(len(buf)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(buf) {
			idx = len(buf) - 1
		}
		return time.Duration(buf[idx])
	}

	var sum int64
	for _, v := range buf {
		sum += v
	}

	return &latencySnapshot{
		Count: total,
		P50:   pct(50),
		P99:   pct(99),
		Max:   time.Duration(buf[len(buf)-1]),
		Avg:   time.Duration(sum / int64(len(buf))),
	}
}

type latencySnapshot struct {
	Count int64
	P50   time.Duration
	P99   time.Duration
	Max   time.Duration
	Avg   time.Duration
}

// log emits one structured JSON line with all percentile stats.
func (s *latencySnapshot) log(l *debugLogger, topic, event string) {
	l.write("info", fmt.Sprintf(
		"KODDI publish latency %s | topic=%s count=%d p50=%s p99=%s max=%s avg=%s",
		event, topic, s.Count,
		fmtDur(s.P50), fmtDur(s.P99), fmtDur(s.Max), fmtDur(s.Avg),
	))
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/1e3)
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
}
