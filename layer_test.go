package layer

import (
	"bytes"
	"encoding/json"
	"fmt"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"io"
	"net/http"
	"strings"
	"testing"

	common_datalayer "github.com/mimiro-io/common-datalayer"
)

func TestStartStopSampleDataLayer(t *testing.T) {

	configFile := "./config" //filepath.Join(filepath.Dir(filename), "config")
	serviceRunner := common_datalayer.NewServiceRunner(NewSparqlDataLayer)
	serviceRunner.WithConfigLocation(configFile)
	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	waitForService("http://127.0.0.1:8090/datasets")

	resp, err := http.Get("http://127.0.0.1:8090/datasets")
	if err != nil {
		t.Error(err)
	}
	content, _ := io.ReadAll(resp.Body)
	expected := `[{"metadata":null,"name":"people","description":""}]`

	if string(content) != expected {
		t.Errorf("Expected %s, got %s", expected, string(content))
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func waitForService(url string) {
	// wait for service to start.
	for {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == 200 {
			break
		}
	}
}

func TestNewSampleDataLayer(t *testing.T) {

	configFile := "./config"

	serviceRunner := common_datalayer.NewServiceRunner(NewSparqlDataLayer)
	serviceRunner.WithConfigLocation(configFile)
	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	waitForService("http://127.0.0.1:8090/datasets")

	// Post data
	reader := strings.NewReader(`[
		{"id": "@context", "namespaces": {"_": "http://data.sample.org/"}},
		{"id": "187", "props": {"name": "John Doe", "age": 42, "likes" : ["apples", "oranges"]}}
	]`)
	resp, err := http.Post("http://127.0.0.1:8090/datasets/people/entities", "application/json", reader)
	if err != nil {
		t.Error(err)
	}

	// Get changes
	resp, err = http.Get("http://127.0.0.1:8090/datasets/people/changes")

	// parse into an EntityCollection
	nsm := egdm.NewNamespaceContext()
	parser := egdm.NewEntityParser(nsm)
	parser.WithExpandURIs()
	ec, err := parser.LoadEntityCollection(resp.Body)
	if err != nil {
		t.Error(err)
	}

	if ec.Entities[0].ID != "http://data.sample.org/187" {
		t.Errorf("Expected http://data.sample.org/187, got %s", ec.Entities[0].ID)
	}

	serviceRunner.Stop()
}

func TestContinuation(t *testing.T) {

	configFile := "./config"

	serviceRunner := common_datalayer.NewServiceRunner(NewSparqlDataLayer)
	serviceRunner.WithConfigLocation(configFile)
	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	waitForService("http://127.0.0.1:8090/datasets")

	// Post data
	reader := strings.NewReader(`[
		{"id": "@context", "namespaces": {"_": "http://data.sample.org/"}},
		{"id": "187", "props": {"name": "John Doe", "age": 42, "likes" : ["apples", "oranges"]}}
	]`)
	resp, err := http.Post("http://127.0.0.1:8090/datasets/people/entities", "application/json", reader)
	if err != nil {
		t.Error(err)
	}

	// Get changes
	resp, err = http.Get("http://127.0.0.1:8090/datasets/people/changes")

	// parse into an EntityCollection
	nsm := egdm.NewNamespaceContext()
	parser := egdm.NewEntityParser(nsm)
	parser.WithExpandURIs()
	ec, err := parser.LoadEntityCollection(resp.Body)
	if err != nil {
		t.Error(err)
	}

	if ec.Entities[0].ID != "http://data.sample.org/187" {
		t.Errorf("Expected http://data.sample.org/187, got %s", ec.Entities[0].ID)
	}

	// Get continuation
	token := ec.Continuation.Token

	// send some new data
	reader = strings.NewReader(`[
		{"id": "@context", "namespaces": {"_": "http://data.sample.org/"}},
		{"id": "189", "props": {"name": "John Doe", "age": 42, "likes" : ["apples", "oranges"]}}
	]`)

	resp, err = http.Post("http://127.0.0.1:8090/datasets/people/entities", "application/json", reader)
	if err != nil {
		t.Error(err)
	}

	// Get changes with continuation
	resp, err = http.Get("http://127.0.0.1:8090/datasets/people/changes?since=" + token)

	// parse into an EntityCollection
	nsm = egdm.NewNamespaceContext()
	parser = egdm.NewEntityParser(nsm)
	parser.WithExpandURIs()
	ec, err = parser.LoadEntityCollection(resp.Body)
	if err != nil {
		t.Error(err)
	}

	if ec.Entities[0].ID != "http://data.sample.org/189" {
		t.Errorf("Expected http://data.sample.org/189, got %s", ec.Entities[0].ID)
	}

	serviceRunner.Stop()
}

func TestFullSync(t *testing.T) {

	configFile := "./config"

	serviceRunner := common_datalayer.NewServiceRunner(NewSparqlDataLayer)
	serviceRunner.WithConfigLocation(configFile)
	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	waitForService("http://127.0.0.1:8090/datasets")

	// Post data
	reader := strings.NewReader(`[
		{"id": "@context", "namespaces": {"_": "http://data.sample.org/"}},
		{"id": "187", "props": {"name": "John Doe", "age": 42, "likes" : ["apples", "oranges"]}}
	]`)

	req, err := http.NewRequest("POST", "http://127.0.0.1:8090/datasets/people/entities", reader)
	if err != nil {
		t.Error("Failed to create request:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("universal-data-api-full-sync-start", "true")
	req.Header.Set("universal-data-api-full-sync-end", "true")
	req.Header.Set("universal-data-api-full-sync-id", "testsync1")

	// Create an HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Error("Request failed:", err)
		return
	}
	defer resp.Body.Close()

	// Get changes
	resp, err = http.Get("http://127.0.0.1:8090/datasets/people/changes")
	_, _ = io.ReadAll(resp.Body)

	serviceRunner.Stop()
}

func TestFullSyncMultiPart(t *testing.T) {

	configFile := "./config"

	serviceRunner := common_datalayer.NewServiceRunner(NewSparqlDataLayer)
	serviceRunner.WithConfigLocation(configFile)
	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	waitForService("http://127.0.0.1:8090/datasets")

	// Post data
	reader := strings.NewReader(`[
		{"id": "@context", "namespaces": {"_": "http://data.sample.org/"}},
		{"id": "187", "props": {"name": "John Doe", "age": 42, "likes" : ["apples", "oranges"]}}
	]`)

	req, err := http.NewRequest("POST", "http://127.0.0.1:8090/datasets/people/entities", reader)
	if err != nil {
		t.Error("Failed to create request:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("universal-data-api-full-sync-start", "true")
	req.Header.Set("universal-data-api-full-sync-end", "false")
	req.Header.Set("universal-data-api-full-sync-id", "testsync1")

	// Create an HTTP client and send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Error("Request failed:", err)
		return
	}
	defer resp.Body.Close()

	reader = strings.NewReader(`[
		{"id": "@context", "namespaces": {"_": "http://data.sample.org/"}},
		{"id": "188", "props": {"name": "John Doe", "age": 42, "likes" : ["apples", "oranges"]}}
	]`)

	req, err = http.NewRequest("POST", "http://127.0.0.1:8090/datasets/people/entities", reader)
	if err != nil {
		t.Error("Failed to create request:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("universal-data-api-full-sync-start", "false")
	req.Header.Set("universal-data-api-full-sync-end", "true")
	req.Header.Set("universal-data-api-full-sync-id", "testsync1")

	// Create an HTTP client and send the request
	client = &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		t.Error("Request failed:", err)
		return
	}
	defer resp.Body.Close()

	// do sparql query count
	count, err := SparqlCountQuery("http://localhost:7200/repositories/test_layer",
		"SELECT ?s ?p ?o FROM <http://example.org/people> WHERE { ?s ?p ?o }")
	if err != nil {
		t.Error("Failed to do sparql query:", err)
		return
	}

	if count != 10 {
		t.Errorf("Expected 8, got %d", count)
	}

	// Get changes
	resp, err = http.Get("http://127.0.0.1:8090/datasets/people/changes")
	_, _ = io.ReadAll(resp.Body)

	serviceRunner.Stop()
}

func SparqlCountQuery(endpoint, query string) (int, error) {
	// Prepare the HTTP request
	req, err := http.NewRequest("POST", endpoint, bytes.NewBufferString(query))
	if err != nil {
		return 0, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/sparql-query")
	req.Header.Set("Accept", "application/sparql-results+json")

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("sending request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("reading response body: %v", err)
	}

	// Parse the JSON response
	var result struct {
		Results struct {
			Bindings []map[string]struct {
				Type  string `json:"type"`
				Value string `json:"value"`
			} `json:"bindings"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, fmt.Errorf("parsing JSON: %v", err)
	}

	// Return the number of rows in the result
	return len(result.Results.Bindings), nil
}
