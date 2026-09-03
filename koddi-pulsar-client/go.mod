module github.com/Prabhanjan-Desai-ibm/koddi-pulsar-client

go 1.21

require (
	github.com/apache/pulsar-client-go v0.0.0
)

// LOCAL DEV: points at the fork root on your machine
// When releasing to Koddi, change this to the GitHub tag:
//   replace github.com/apache/pulsar-client-go => github.com/Prabhanjan-Desai-ibm/pulsar-client-go v1.0.0
replace github.com/apache/pulsar-client-go => ../
