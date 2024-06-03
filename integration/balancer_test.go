package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func GetBaseAddress() string {
	hostname := os.Getenv("BALANCER_HOST")
	if hostname == "" {
		hostname = "localhost"
	}

	// Define baseAddress with the hostname
	return fmt.Sprintf("http://%s:8090", hostname)
}

var baseAddress = GetBaseAddress()

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	servers := map[string]bool{}
	for i := 0; i < 10; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=QuantumGurus", baseAddress))
		if err != nil {
			t.Error(err)
		}
		server := resp.Header.Get("lb-from")
		servers[server] = true
		err = resp.Body.Close()
		if err != nil {
			return
		}
	}

	assert.True(t, len(servers) > 1, "Requests were not distributed across multiple servers")
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=QuantumGurus", baseAddress))
		if err != nil {
			b.Error(err)
		}
		err = resp.Body.Close()
		if err != nil {
			return
		}
	}
}

func TestLoadBalancerMultipleServers(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}
	serverResponses := make(map[string]bool)

	for i := 0; i < 10; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=QuantumGurus", baseAddress))
		if err != nil {
			t.Fatalf("Failed to send request to load balancer: %v", err)
		}

		lbFrom := resp.Header.Get("lb-from")
		if lbFrom == "" {
			t.Fatalf("Expected lb-from header, got none")
		}

		serverResponses[lbFrom] = true
		err = resp.Body.Close()
		if err != nil {
			return
		}
	}

	assert.GreaterOrEqual(t, len(serverResponses), 2, "Expected responses from at least 2 different servers")
	fmt.Println("Test passed, responses received from different servers:", serverResponses)
}
