package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/QuantumGurus/Lab4-KPI/httptools"
	"github.com/QuantumGurus/Lab4-KPI/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

func main() {
	setDBKeyValuePair("QuantumGurus", getCurrentDate())

	h := new(http.ServeMux)
	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()

		key := query.Get("key")
		value, err := getDBValueByKey(key)
		if err != nil {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode(
			map[string]string{"key": key, "value": value},
		)
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

func setDBKeyValuePair(key, value string) {
	dbSetEndpoint := fmt.Sprintf("http://db:8080/db/%s", key)

	requestMapping := map[string]string{"value": value}
	requestJSON, _ := json.Marshal(requestMapping)

	req, _ := http.NewRequest("POST", dbSetEndpoint, bytes.NewBuffer(requestJSON))
	req.Header.Set("Content-Type", "application/json")

	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
}

func getDBValueByKey(key string) (string, error) {
	dbGetEndpoint := fmt.Sprintf("http://db:8080/db/%s", key)

	req, _ := http.NewRequest("GET", dbGetEndpoint, bytes.NewBuffer(nil))
	resp, respErr := http.DefaultClient.Do(req)

	if respErr != nil {
		return "", respErr
	}

	defer resp.Body.Close()

	var responseKVPair map[string]string

	err := json.NewDecoder(resp.Body).Decode(&responseKVPair)

	if err != nil {
		return "", err
	}

	value, isFieldPresent := responseKVPair["value"]
	if !isFieldPresent {
		return "", errors.New("value not found in response")
	}

	return value, nil
}

func getCurrentDate() string {
	return time.Now().Format("2006-01-02")
}
