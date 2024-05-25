package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetLeastTrafficServer(t *testing.T) {
	traffic = map[string]int{
		"server1:8080": 100,
		"server2:8080": 50,
		"server3:8080": 200,
	}
	server := getLeastTrafficServer()
	assert.Equal(t, "server2:8080", server)
}

func TestForward(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Write([]byte("OK"))
	}))
	defer server.Close()

	serversPool = []string{server.URL[7:]}
	traffic[server.URL[7:]] = 0

	rw := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	err := forward(server.URL[7:], rw, req)
	assert.Nil(t, err)
	assert.Equal(t, "OK", rw.Body.String())
}
