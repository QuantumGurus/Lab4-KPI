package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	servers := map[string]bool{}
	for i := 0; i < 10; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			t.Error(err)
		}
		server := resp.Header.Get("lb-from")
		servers[server] = true
		resp.Body.Close()
	}

	assert.True(t, len(servers) > 1, "Requests were not distributed across multiple servers")
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Error(err)
		}
		resp.Body.Close()
	}
}
