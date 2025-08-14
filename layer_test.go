package layer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	common_datalayer "github.com/mimiro-io/common-datalayer"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	fusekiContainer      testcontainers.Container
	sparqlQueryEndpoint  string
	sparqlUpdateEndpoint string
	testConfigDir        string
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "stain/jena-fuseki",
		ExposedPorts: []string{"3030/tcp"},
		Env: map[string]string{
			"FUSEKI_DATASET_1": "test_layer",
		},
		WaitingFor: wait.ForHTTP("/$/ping").WithPort("3030/tcp").WithStartupTimeout(2 * time.Minute),
	}

	var err error
	fusekiContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		log.Fatalf("failed to start fuseki container: %v", err)
	}

	port, err := fusekiContainer.MappedPort(ctx, "3030/tcp")
	if err != nil {
		log.Fatalf("failed to get mapped port: %v", err)
	}
	host, err := fusekiContainer.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get host: %v", err)
	}

	base := fmt.Sprintf("http://%s:%s/test_layer", host, port.Port())
	sparqlQueryEndpoint = base + "/sparql"
	sparqlUpdateEndpoint = base + "/update"

	testConfigDir, err = createConfigDir(sparqlQueryEndpoint, sparqlUpdateEndpoint)
	if err != nil {
		log.Fatalf("failed to create config: %v", err)
	}

	code := m.Run()

	if err := fusekiContainer.Terminate(ctx); err != nil {
		log.Printf("failed to terminate container: %v", err)
	}

	os.Exit(code)
}

func createConfigDir(queryEndpoint, updateEndpoint string) (string, error) {
	data, err := os.ReadFile("config/config.json")
	if err != nil {
		return "", err
	}

	var cfg map[string]any
	if err := json.Unmarshal(data, &cfg); err != nil {
		return "", err
	}

	sc := cfg["system_config"].(map[string]any)
	sc["sparql_query_endpoint"] = queryEndpoint
	sc["sparql_update_endpoint"] = updateEndpoint

	dir, err := os.MkdirTemp("", "config")
	if err != nil {
		return "", err
	}

	b, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}

	if err := os.WriteFile(filepath.Join(dir, "config.json"), b, 0o644); err != nil {
		return "", err
	}

	return dir, nil
}

func TestStartStopSampleDataLayer(t *testing.T) {

	configFile := testConfigDir //filepath.Join(filepath.Dir(filename), "config")
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

	configFile := testConfigDir

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

	configFile := testConfigDir

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

	configFile := testConfigDir

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
	count, err := SparqlCountQuery(sparqlQueryEndpoint,
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
