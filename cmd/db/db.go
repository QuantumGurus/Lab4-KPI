package main

import (
	"encoding/json"
	"github.com/QuantumGurus/Lab4-KPI/datastore"
	"log"
	"net/http"
	"os"
)

var db *datastore.Db

func main() {
	var err error

	CreateDirIfNotExist("db_data")
	db, err = datastore.NewDatabase("db_data", 1024*1024)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}

	http.HandleFunc("GET /db/{key}", dbGetHandler)
	http.HandleFunc("POST /db/{key}", dbPostHandler)

	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting DB server on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func dbGetHandler(responseWriter http.ResponseWriter, req *http.Request) {
	key := req.PathValue("key")
	value, err := db.Get(key)
	if err != nil {
		responseWriter.WriteHeader(http.StatusNotFound)
		return
	}

	response := map[string]string{"key": key, "value": value}

	encodingErr := json.NewEncoder(responseWriter).Encode(response)
	if encodingErr != nil {
		responseWriter.WriteHeader(http.StatusInternalServerError)
	}
}

func CreateDirIfNotExist(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.Mkdir(dir, os.ModePerm)
		if err != nil {
			return
		}
	}
}

func dbPostHandler(responseWriter http.ResponseWriter, req *http.Request) {
	key := req.PathValue("key")
	var request map[string]string

	if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
		http.Error(responseWriter, "Invalid request body", http.StatusBadRequest)
		return
	}

	value, isFieldPresent := request["value"]
	if !isFieldPresent {
		http.Error(responseWriter, "Value is missing", http.StatusBadRequest)
		return
	}

	putErr := db.Put(key, value)
	if putErr != nil {
		responseWriter.WriteHeader(http.StatusInternalServerError)
	}
}
