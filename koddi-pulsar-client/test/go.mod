module github.com/Prabhanjan-Desai-ibm/koddi-pulsar-client-test

go 1.21

require (
	github.com/Prabhanjan-Desai-ibm/koddi-pulsar-client v1.0.0
	github.com/apache/pulsar-client-go v0.21.1
)

// Resolve the wrapper from its tagged release
replace github.com/Prabhanjan-Desai-ibm/koddi-pulsar-client => github.com/Prabhanjan-Desai-ibm/pulsar-client-go/koddi-pulsar-client v1.0.0

// Resolve the fork (same as in wrapper's go.mod)
replace github.com/apache/pulsar-client-go => github.com/Prabhanjan-Desai-ibm/pulsar-client-go v0.21.1
