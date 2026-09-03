package koddi

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	pulsarlog "github.com/apache/pulsar-client-go/pulsar/log"
)

// debugLogger implements pulsar/log.Logger.
// Every log line is written as a JSON object to stdout so it lands
// in Splunk exactly like the proxy logs Koddi already collects.
// Fields added to every line:
//   - ts         : RFC3339 timestamp
//   - level      : debug / info / warn / error
//   - source     : "koddi-pulsar-client"  (easy Splunk filter)
//   - cluster    : value passed to NewClient
//   - msg        : the actual message from the Go client internals
//   - any extra  : fields added via WithField / WithFields / WithError
type debugLogger struct {
	cluster string
	fields  map[string]interface{}
}

func newDebugLogger(cluster string) pulsarlog.Logger {
	return &debugLogger{
		cluster: cluster,
		fields:  map[string]interface{}{},
	}
}

// --- pulsarlog.Logger interface ---

func (l *debugLogger) SubLogger(fields pulsarlog.Fields) pulsarlog.Logger {
	// merge parent fields with new fields into a fresh logger
	merged := make(map[string]interface{}, len(l.fields)+len(fields))
	for k, v := range l.fields {
		merged[k] = v
	}
	for k, v := range fields {
		merged[k] = v
	}
	return &debugLogger{cluster: l.cluster, fields: merged}
}

func (l *debugLogger) WithFields(fields pulsarlog.Fields) pulsarlog.Entry {
	return l.SubLogger(fields).(*debugLogger)
}

func (l *debugLogger) WithField(name string, value interface{}) pulsarlog.Entry {
	return l.WithFields(pulsarlog.Fields{name: value})
}

func (l *debugLogger) WithError(err error) pulsarlog.Entry {
	return l.WithFields(pulsarlog.Fields{"error": err.Error()})
}

func (l *debugLogger) Debug(args ...interface{}) { l.write("debug", fmt.Sprint(args...)) }
func (l *debugLogger) Info(args ...interface{})  { l.write("info", fmt.Sprint(args...)) }
func (l *debugLogger) Warn(args ...interface{})  { l.write("warn", fmt.Sprint(args...)) }
func (l *debugLogger) Error(args ...interface{}) { l.write("error", fmt.Sprint(args...)) }

func (l *debugLogger) Debugf(format string, args ...interface{}) {
	l.write("debug", fmt.Sprintf(format, args...))
}
func (l *debugLogger) Infof(format string, args ...interface{}) {
	l.write("info", fmt.Sprintf(format, args...))
}
func (l *debugLogger) Warnf(format string, args ...interface{}) {
	l.write("warn", fmt.Sprintf(format, args...))
}
func (l *debugLogger) Errorf(format string, args ...interface{}) {
	l.write("error", fmt.Sprintf(format, args...))
}

// --- pulsarlog.Entry interface (same methods, debugLogger implements both) ---

// write serialises one log line as JSON to stdout.
func (l *debugLogger) write(level, msg string) {
	line := map[string]interface{}{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"level":   level,
		"source":  "koddi-pulsar-client",
		"cluster": l.cluster,
		"msg":     msg,
	}
	// merge extra fields (remote_addr, topic, producerID etc from SubLogger calls)
	for k, v := range l.fields {
		line[k] = v
	}
	b, _ := json.Marshal(line)
	fmt.Fprintln(os.Stdout, string(b))
}
