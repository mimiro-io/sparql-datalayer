package layer

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	cdl "github.com/mimiro-io/common-datalayer"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"text/template"
)

var insecureHTTPClient = &http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	},
}

func EnrichConfig(args []string, config *cdl.Config) error {
	err := cdl.BuildNativeSystemEnvOverrides(
		cdl.Env("sparql_query_endpoint"),
		cdl.Env("sparql_update_endpoint"),
		cdl.Env("sparql_user", "auth_user"),
		cdl.Env("sparql_secret", "auth_secret"),
		cdl.Env("sparql_auth_type", "auth_type"),
		cdl.Env("sparql_auth_endpoint", "auth_endpoint"),
	)(config)
	if err != nil {
		return err
	}

	auth := map[string]any{}
	if existing, ok := config.NativeSystemConfig["auth"].(map[string]any); ok {
		auth = existing
	}
	if v, ok := config.NativeSystemConfig["auth_user"]; ok {
		auth["user"] = v
	}
	if v, ok := config.NativeSystemConfig["auth_secret"]; ok {
		auth["secret"] = v
	}
	if v, ok := config.NativeSystemConfig["auth_type"]; ok {
		auth["type"] = v
	}
	if v, ok := config.NativeSystemConfig["auth_endpoint"]; ok {
		auth["endpoint"] = v
	}
	if len(auth) > 0 {
		config.NativeSystemConfig["auth"] = auth
	}
	delete(config.NativeSystemConfig, "auth_user")
	delete(config.NativeSystemConfig, "auth_secret")
	delete(config.NativeSystemConfig, "auth_type")
	delete(config.NativeSystemConfig, "auth_endpoint")
	return nil
}

func NewSparqlDataLayer(config *cdl.Config, logger cdl.Logger, metrics cdl.Metrics) (cdl.DataLayerService, error) {
	sdl := &SparqlDataLayer{logger: logger, metrics: metrics}
	err := sdl.UpdateConfiguration(config)
	if err != nil {
		sdl.logger.Error("Failed to update configuration", "err", err)
		return nil, err
	}
	sdl.logger.Info("Sparql data layer initialized",
		"queryEndpoint", sdl.store.config.SparqlQueryEndpoint,
		"updateEndpoint", sdl.store.config.SparqlUpdateEndpoint,
		"datasets", len(sdl.datasets))
	return sdl, nil
}

type SparqlDataLayer struct {
	config   *cdl.Config
	logger   cdl.Logger
	metrics  cdl.Metrics
	datasets map[string]*SparqlDataset
	store    *SparqlStore
}

type SparqlDataLayerConfig struct {
	SparqlQueryEndpoint  string              `json:"sparql_query_endpoint"`
	SparqlUpdateEndpoint string              `json:"sparql_update_endpoint"`
	Auth                 *SparqlEndpointAuth `json:"auth"`
}

type SparqlEndpointAuth struct {
	User     string `json:"user"`
	Secret   string `json:"secret"`
	Type     string `json:"type"`
	Endpoint string `json:"endpoint"`
}

func (s *SparqlDataLayer) DatasetDescriptions() []*cdl.DatasetDescription {
	// iterate datasets and return descriptions
	descriptions := make([]*cdl.DatasetDescription, 0)
	for _, dataset := range s.datasets {
		descriptions = append(descriptions, &cdl.DatasetDescription{Name: dataset.name})
	}
	return descriptions
}

func (s *SparqlDataLayer) Stop(ctx context.Context) error {
	return nil
}

func (s *SparqlDataLayer) Dataset(dataset string) (cdl.Dataset, cdl.LayerError) {
	// get dataset from datasets map
	if s.datasets != nil {
		if ds, exists := s.datasets[dataset]; exists {
			s.logger.Debug("Dataset lookup", "dataset", dataset, "found", true)
			return ds, nil
		}
		s.logger.Warn("Dataset not found", "dataset", dataset)
		return nil, cdl.Err(errors.New("dataset not found"), cdl.LayerErrorInternal)
	}
	s.logger.Error("Dataset map not initialized")
	return nil, cdl.Err(errors.New("dataset not found"), cdl.LayerErrorInternal)
}

func (s *SparqlDataLayer) DatasetNames() []string {
	names := make([]string, 0)
	for _, dataset := range s.config.DatasetDefinitions {
		names = append(names, dataset.DatasetName)
	}
	return names
}

func (s *SparqlDataLayer) UpdateConfiguration(config *cdl.Config) cdl.LayerError {
	s.config = config
	s.datasets = make(map[string]*SparqlDataset)

	// parse native config into struct
	var sparqlConfig SparqlDataLayerConfig
	rawJson, _ := json.Marshal(config.NativeSystemConfig)
	json.Unmarshal(rawJson, &sparqlConfig)

	// create sparql store
	s.store = &SparqlStore{logger: s.logger, config: &sparqlConfig}
	s.logger.Info("Configuring SPARQL data layer",
		"queryEndpoint", sparqlConfig.SparqlQueryEndpoint,
		"updateEndpoint", sparqlConfig.SparqlUpdateEndpoint,
		"datasets", len(config.DatasetDefinitions))

	// create datasets
	for _, definition := range config.DatasetDefinitions {
		// parse dataset config into struct
		var datasetConfig SparqlDatasetConfig
		rawJson, _ = json.Marshal(definition.SourceConfig)
		json.Unmarshal(rawJson, &datasetConfig)

		dataset := NewSparqlDataset(definition.DatasetName, s.logger, s.store, &datasetConfig)
		s.datasets[dataset.name] = dataset
		s.logger.Debug("Configured dataset", "dataset", dataset.name)
	}
	return nil
}

type SparqlStore struct {
	logger cdl.Logger
	config *SparqlDataLayerConfig
}

// SparqlDataset is a dataset that is backed by a SPARQL endpoint and implements the Dataset interface
type SparqlDataset struct {
	logger cdl.Logger
	name   string
	store  *SparqlStore
	config *SparqlDatasetConfig
}

type SparqlDatasetConfig struct {
	Graph                  string `json:"graph"`
	SnapshotQuerySchedule  string `json:"snapshot_query_schedule"`
	SnapshotSyncQuery      string `json:"snapshot_sync_query"`
	IncrementalQuery       string `json:"incremental_query"`
	LatestItemQuery        string `json:"latest_item_query"`
	WriteBatchSize         int    `json:"write_batch_size"`
	LastModifiedPredicate  string `json:"last_modified_predicate"`
	FullSyncUpdateStrategy string `json:"full_sync_update_strategy"`
}

func NewSparqlDataset(name string, logger cdl.Logger, store *SparqlStore, config *SparqlDatasetConfig) *SparqlDataset {
	return &SparqlDataset{name: name, logger: logger, store: store, config: config}
}

const changesQuery = `
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?s ?p ?o
WHERE {
  GRAPH <{{.Graph}}> {
    ?s ?p ?o .
  }
}
ORDER BY ?s`

const changesQueryWithModifiedDate = `
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?s ?p ?o
WHERE {
  GRAPH <{{.Graph}}> {
    ?s ?p ?o .
    ?s <{{.ModifiedPredicate}}> ?lastModified .
    FILTER (?lastModified > "{{.Since}}"^^xsd:dateTime)
  }
}
ORDER BY ?s ?lastModified`

const maxLastModifiedQuery = `
SELECT (MAX(?lastModified) AS ?latestModification)
WHERE {
  GRAPH <{{.Graph}}> {
    ?s <{{.ModifiedPredicate}}> ?lastModified .
  }
}`

type ChangesQueryParams struct {
	Graph             string
	ModifiedPredicate string
	Since             string
}

func (s *SparqlDataset) Changes(since string, take int, latestOnly bool) (cdl.EntityIterator, cdl.LayerError) {
	s.logger.Debug("Changes called",
		"dataset", s.name,
		"since", since,
		"take", take,
		"latestOnly", latestOnly)
	if since != "" {
		// decode from base 64
		decoded, err := base64.StdEncoding.DecodeString(since)
		if err != nil {
			s.logger.Warn("Invalid since token", "dataset", s.name, "err", err)
			return nil, cdl.Err(err, cdl.LayerErrorBadParameter)
		}
		since = string(decoded)
	}

	// create changes params
	params := ChangesQueryParams{
		Graph:             s.config.Graph,
		ModifiedPredicate: s.config.LastModifiedPredicate,
		Since:             since,
	}

	// run maxlastmodified by using template
	var maxLastModifiedQueryBuffer bytes.Buffer
	t := template.Must(template.New("maxLastModifiedQuery").Parse(maxLastModifiedQuery))
	err := t.Execute(&maxLastModifiedQueryBuffer, params)
	if err != nil {
		s.logger.Error("Failed to build max last modified query", "dataset", s.name, "err", err)
		return nil, cdl.Err(err, cdl.LayerErrorInternal)
	}
	s.logger.Debug("Executing max last modified query",
		"dataset", s.name,
		"query", maxLastModifiedQueryBuffer.String())

	// execute the SPARQL query
	result, err := doSparqlQuery(s.logger, s.store.config.SparqlQueryEndpoint, maxLastModifiedQueryBuffer.String(), s.store.config.Auth)
	if err != nil {
		s.logger.Error("Max last modified query failed", "dataset", s.name, "err", err)
		return nil, cdl.Err(err, cdl.LayerErrorInternal)
	}

	// get the latest modification date to be used as the since token returned
	latestModification := result.Results.Bindings[0]["latestModification"].Value
	s.logger.Debug("Latest modification", "dataset", s.name, "value", latestModification)

	resultsChan := make(chan SPARQLBinding)

	// run changes query by using template

	query := changesQuery
	if since != "" {
		query = changesQueryWithModifiedDate
	}

	var changesQueryBuffer bytes.Buffer
	t = template.Must(template.New("changesQuery").Parse(query))
	err = t.Execute(&changesQueryBuffer, params)
	if err != nil {
		s.logger.Error("Failed to build changes query", "dataset", s.name, "err", err)
		return nil, cdl.Err(err, cdl.LayerErrorInternal)
	}
	s.logger.Debug("Executing changes query",
		"dataset", s.name,
		"query", changesQueryBuffer.String())

	go func() {
		if err := fetchSPARQLResults(s.logger, s.store.config.SparqlQueryEndpoint, changesQueryBuffer.String(), s.store.config.Auth, resultsChan); err != nil {
			s.logger.Error("Fetching SPARQL results failed", "dataset", s.name, "err", err)
		}
	}()

	iterator := &SparqlEntityIterator{results: result, lastModified: latestModification, resultsChan: resultsChan, done: false}
	return iterator, nil
}

type SparqlEntityIterator struct {
	results      *SPARQLResult
	lastModified string
	resultsChan  <-chan SPARQLBinding
	currentBatch []SPARQLBinding
	lastSubject  string
	done         bool
}

func (sei *SparqlEntityIterator) Close() cdl.LayerError {
	// nothing to do
	return nil
}

func (sei *SparqlEntityIterator) Context() *egdm.Context {
	ctx := egdm.NewNamespaceContext()
	return ctx.AsContext()
}

// Next returns the next group of results for the current subject.
func (sei *SparqlEntityIterator) Next() (*egdm.Entity, cdl.LayerError) {
	if sei.done {
		return nil, nil
	}

	for binding := range sei.resultsChan {
		currentSubject := binding["s"].Value
		if sei.lastSubject != "" && sei.lastSubject != currentSubject {
			sei.lastSubject = currentSubject
			entity, err := makeEntityFromBindings(sei.currentBatch)
			if err != nil {
				return nil, cdl.Err(err, cdl.LayerErrorInternal)
			}
			sei.currentBatch = []SPARQLBinding{binding}
			return entity, nil
		}
		sei.lastSubject = currentSubject
		sei.currentBatch = append(sei.currentBatch, binding)
	}

	if len(sei.currentBatch) > 0 { // Final batch
		sei.done = true
		entity, err := makeEntityFromBindings(sei.currentBatch)
		if err != nil {
			return nil, cdl.Err(err, cdl.LayerErrorInternal)
		}
		sei.currentBatch = nil
		return entity, nil
	}

	return nil, nil
}

func makeEntityFromBindings(bindings []SPARQLBinding) (*egdm.Entity, error) {
	// given a map of bindings where the keys are s, p, o, create an entity
	// check the o to see if it is a literal or a resource
	// if it is a resource, add it to the references
	// if it is a literal, add it to the properties

	entity := &egdm.Entity{}
	entity.Properties = make(map[string]any)
	entity.References = make(map[string]any)

	entity.ID = bindings[0]["s"].Value

	for _, binding := range bindings {
		predicate := binding["p"].Value
		object := binding["o"].Value

		if binding["o"].Type == "uri" {
			// Check if the key exists in References
			if existing, ok := entity.References[predicate]; ok {
				var list []string
				// Check if there is already a list
				if currentList, isList := existing.([]string); isList {
					list = append(currentList, object)
				} else {
					list = []string{existing.(string), object}
				}
				entity.References[predicate] = list
			} else {
				entity.References[predicate] = object
			}
		} else {
			// Check if the key exists in Properties
			if existing, ok := entity.Properties[predicate]; ok {
				var list []string
				// Check if there is already a list
				if currentList, isList := existing.([]string); isList {
					list = append(currentList, object)
				} else {
					list = []string{existing.(string), object}
				}
				entity.Properties[predicate] = list
			} else {
				entity.Properties[predicate] = object
			}
		}
	}

	return entity, nil
}

func fetchSPARQLResults(logger cdl.Logger, endpoint string, query string, auth *SparqlEndpointAuth, results chan<- SPARQLBinding) error {
	defer close(results)

	logger.Debug("Fetching SPARQL results", "endpoint", endpoint, "query", query)

	// Prepare the HTTP request
	req, err := http.NewRequest("POST", endpoint, bytes.NewBufferString(query))
	if err != nil {
		logger.Error("Failed to create request", "endpoint", endpoint, "err", err)
		return err
	}
	req.Header.Set("Content-Type", "application/sparql-query")
	req.Header.Set("Accept", "application/sparql-results+json")

	if auth != nil && strings.ToLower(auth.Type) == "basic" && auth.User != "" {
		req.SetBasicAuth(auth.User, auth.Secret)
	}

	// Send the request
	resp, err := insecureHTTPClient.Do(req)
	if err != nil {
		logger.Error("Failed to fetch results", "endpoint", endpoint, "err", err)
		return err
	}
	defer resp.Body.Close()

	// Create a JSON decoder
	decoder := json.NewDecoder(resp.Body)
	foundBindings := false
	for {
		t, err := decoder.Token()
		if err != nil {
			if err == io.EOF {
				break
			}
			logger.Error("Reading JSON token failed", "endpoint", endpoint, "err", err)
			return fmt.Errorf("reading JSON token: %v", err)
		}

		if delim, ok := t.(json.Delim); ok && delim == '[' {
			if foundBindings {
				break // Start of array
			}
		} else if key, ok := t.(string); ok && key == "bindings" {
			foundBindings = true // Next token should be the array start
		}
	}

	// Decode each binding in the array
	for decoder.More() {
		var binding SPARQLBinding
		if err := decoder.Decode(&binding); err != nil {
			logger.Error("Decoding binding failed", "endpoint", endpoint, "err", err)
			return fmt.Errorf("decoding binding: %v", err)
		}
		results <- binding
	}

	return nil
}

func (sei *SparqlEntityIterator) Token() (*egdm.Continuation, cdl.LayerError) {
	cont := egdm.NewContinuation()
	tok64 := base64.StdEncoding.EncodeToString([]byte(sei.lastModified))
	cont.Token = tok64
	return cont, nil
}

func (s *SparqlDataset) Entities(since string, take int) (cdl.EntityIterator, cdl.LayerError) {
	return s.Changes(since, take, true)
}

func (s *SparqlDataset) MetaData() map[string]any {
	meta := make(map[string]any)
	return meta
}

func (s *SparqlDataset) Name() string {
	return s.name
}

func (s *SparqlDataset) FullSync(ctx context.Context, batchInfo cdl.BatchInfo) (cdl.DatasetWriter, cdl.LayerError) {
	s.logger.Info("Starting full sync", "dataset", s.name, "syncId", batchInfo.SyncId)

	sdw := NewSparqlDatasetWriter(s, s.logger, batchInfo, true, s.config.WriteBatchSize)

	// if first batch then we need to clear the graph / tmp graph
	if batchInfo.IsStartBatch {
		if s.config.FullSyncUpdateStrategy == "truncate" {
			s.logger.Debug("Clearing graph for full sync", "dataset", s.name, "strategy", "truncate")
			dropGraphQuery := fmt.Sprintf("CLEAR SILENT GRAPH <%s>", s.config.Graph)
			err := sendSparqlUpdate(s.store.config.SparqlUpdateEndpoint, dropGraphQuery, s.store.config.Auth, s.logger)
			if err != nil {
				s.logger.Error("Failed to clear graph", "dataset", s.name, "err", err)
				return nil, cdl.Err(err, cdl.LayerErrorInternal)
			}
		} else {
			s.logger.Debug("Clearing temp graph for full sync", "dataset", s.name, "strategy", "tmp")
			dropGraphQuery := fmt.Sprintf("CLEAR SILENT GRAPH <%s>", sdw.fullSyncTempGraph)
			err := sendSparqlUpdate(s.store.config.SparqlUpdateEndpoint, dropGraphQuery, s.store.config.Auth, s.logger)
			if err != nil {
				s.logger.Error("Failed to clear temp graph", "dataset", s.name, "err", err)
				return nil, cdl.Err(err, cdl.LayerErrorInternal)
			}
		}
	}

	return sdw, nil
}

func (s *SparqlDataset) Incremental(ctx context.Context) (cdl.DatasetWriter, cdl.LayerError) {
	s.logger.Info("Starting incremental sync", "dataset", s.name)
	sdw := NewSparqlDatasetWriter(s, s.logger, cdl.BatchInfo{}, false, s.config.WriteBatchSize)
	return sdw, nil
}

func NewSparqlDatasetWriter(dataset *SparqlDataset, logger cdl.Logger, batchInfo cdl.BatchInfo, fullSync bool, writeBatchSize int) *SparqlDatasetWriter {
	tmpGraphUri := "http://integration.rdf.io/" + dataset.name + "/" + batchInfo.SyncId
	return &SparqlDatasetWriter{logger: logger,
		batchInfo:         &batchInfo,
		isFullSync:        fullSync,
		batchSize:         writeBatchSize,
		dataset:           dataset,
		fullSyncTempGraph: tmpGraphUri,
		fullSyncStrategy:  TmpGraph}
}

type UpdateStrategy int

const (
	DeleteAndReplace UpdateStrategy = iota + 1
	QueryForDiff
)

type FullSyncStrategy int

const (
	TmpGraph      FullSyncStrategy = iota + 1 // store all data in a tmp graph then rename it
	TruncateGraph                             // delete all data in the graph and then insert new data
)

type SparqlDatasetWriter struct {
	logger            cdl.Logger
	batchInfo         *cdl.BatchInfo
	dataset           *SparqlDataset
	written           int      // how many entities have been written so far
	batchSize         int      // number of entities to process before writing to the store
	batchResources    []string // list of the resources that are being modified
	batchGraph        []string // current set of statements to insert after delete
	updateStrategy    UpdateStrategy
	fullSyncTempGraph string
	fullSyncStrategy  FullSyncStrategy // how to handle full sync
	isFullSync        bool
}

// define all the constants for the XSD types
const (
	XsdInteger      = "<http://www.w3.org/2001/XMLSchema#integer>"
	XsdInt          = "<http://www.w3.org/2001/XMLSchema#int>"
	XsdLong         = "<http://www.w3.org/2001/XMLSchema#long>"
	XsdAnyURI       = "<http://www.w3.org/2001/XMLSchema#anyURI>"
	XsdBase64Binary = "<http://www.w3.org/2001/XMLSchema#base64Binary>"
	XsdBoolean      = "<http://www.w3.org/2001/XMLSchema#boolean>"
	XsdDate         = "<http://www.w3.org/2001/XMLSchema#date>"
	XsdDateTime     = "<http://www.w3.org/2001/XMLSchema#dateTime>"
	XsdDecimal      = "<http://www.w3.org/2001/XMLSchema#decimal>"
	XsdDouble       = "<http://www.w3.org/2001/XMLSchema#double>"
	XsdDuration     = "<http://www.w3.org/2001/XMLSchema#duration>"
	XsdFloat        = "<http://www.w3.org/2001/XMLSchema#float>"
	XsdGDay         = "<http://www.w3.org/2001/XMLSchema#gDay>"
	XsdGMonth       = "<http://www.w3.org/2001/XMLSchema#gMonth>"
	XsdGMonthDay    = "<http://www.w3.org/2001/XMLSchema#gMonthDay>"
	XsdGYear        = "<http://www.w3.org/2001/XMLSchema#gYear>"
	XsdGYearMonth   = "<http://www.w3.org/2001/XMLSchema#gYearMonth>"
	XsdHexBinary    = "<http://www.w3.org/2001/XMLSchema#hexBinary>"
	XsdQName        = "<http://www.w3.org/2001/XMLSchema#QName>"
	XsdString       = "<http://www.w3.org/2001/XMLSchema#string>"
	XsdTime         = "<http://www.w3.org/2001/XMLSchema#time>"
)

func (dsw *SparqlDatasetWriter) Write(entity *egdm.Entity) cdl.LayerError {

	if dsw.batchGraph == nil {
		dsw.batchGraph = make([]string, 0)
		dsw.batchResources = make([]string, 0)
		dsw.written = 0
	}

	// make subject id
	subject := "<" + entity.ID + ">"
	dsw.batchResources = append(dsw.batchResources, subject)

	// increment written count
	dsw.written++

	dsw.logger.Debug("Queueing entity", "dataset", dsw.dataset.name, "id", entity.ID, "deleted", entity.IsDeleted)

	// if entity is deleted, we omit it from the update graph
	if !entity.IsDeleted {
		// do properties
		for prop, value := range entity.Properties {
			predicate := prop
			// check if value is a list or single value
			if isArray(value) {
				anyValues := convertToArrayOfAny(value)
				for _, v := range anyValues {
					object, ok := makeNTLiteralString(v)
					if !ok {
						dsw.logger.Warn("Unsupported property type", "dataset", dsw.dataset.name, "predicate", predicate, "type", reflect.TypeOf(v).String())
						continue
					}
					t := subject + " <" + predicate + "> " + object + " .\n"
					dsw.batchGraph = append(dsw.batchGraph, t)
				}
			} else {
				object, ok := makeNTLiteralString(value)
				if ok {
					t := subject + " <" + predicate + "> " + object + " .\n"
					dsw.batchGraph = append(dsw.batchGraph, t)
				} else {
					dsw.logger.Warn("Unsupported property type", "dataset", dsw.dataset.name, "predicate", predicate, "type", reflect.TypeOf(value).String())
				}
			}
		}

		// do references
		for prop, value := range entity.References {
			predicate := prop
			// check if value is a list or single value
			if isArray(value) {
				anyValues := convertToArrayOfAny(value)
				for _, v := range anyValues {
					object := "<" + v.(string) + ">"
					t := subject + " <" + predicate + "> " + object + " .\n"
					dsw.batchGraph = append(dsw.batchGraph, t)
				}
			} else {
				object := "<" + value.(string) + ">"
				t := subject + " <" + predicate + "> " + object + " .\n"
				dsw.batchGraph = append(dsw.batchGraph, t)
			}
		}
	}

	// the last modified date is recorded even if the item is deleted
	t := subject + " <" + dsw.dataset.config.LastModifiedPredicate + "> " + "?now" + ".\n"
	dsw.batchGraph = append(dsw.batchGraph, t)

	if dsw.written == dsw.batchSize {
		dsw.logger.Debug("Batch size reached, flushing", "dataset", dsw.dataset.name, "size", dsw.written)
		err := dsw.Flush()
		if err != nil {
			dsw.logger.Error("Flush failed", "dataset", dsw.dataset.name, "err", err)
			return err
		}
	}

	return nil
}

func convertToArrayOfAny(value any) []any {
	val := reflect.ValueOf(value)
	result := make([]interface{}, val.Len())
	for i := 0; i < val.Len(); i++ {
		result[i] = val.Index(i).Interface()
	}
	return result
}

func isArray(value interface{}) bool {
	// Get the reflection type object of the value
	valueType := reflect.TypeOf(value)

	// Check if the kind of the type is Array
	kind := valueType.Kind()
	return kind == reflect.Array || kind == reflect.Slice
}

func makeNTLiteralString(value any) (string, bool) {
	// switch on value type
	switch v := value.(type) {
	case string:
		return "\"" + v + "\"", true
	case int:
		return "\"" + strconv.Itoa(v) + "\"" + "^^" + XsdInteger, true
	case bool:
		if v {
			return "\"true\"" + "^^" + XsdBoolean, true
		} else {
			return "\"false\"" + "^^" + XsdBoolean, true
		}
	case float64:
		return "\"" + strconv.FormatFloat(v, 'f', -1, 64) + "\"" + "^^" + XsdDouble, true
	case int64:
		return "\"" + strconv.FormatInt(v, 10) + "\"" + "^^" + XsdLong, true
	case int32:
		return "\"" + strconv.FormatInt(int64(v), 10) + "\"" + "^^" + XsdInt, true
	default:
		return "", false
	}
}

func (dsw *SparqlDatasetWriter) Flush() cdl.LayerError {
	var updateData UpdateData
	updateData.Graph = dsw.dataset.config.Graph

	if dsw.isFullSync {
		if dsw.fullSyncStrategy == TmpGraph {
			updateData.Graph = dsw.fullSyncTempGraph
		}
	}

	// make string of the graph triples
	var builder strings.Builder
	builder.Grow(128 * len(dsw.batchGraph))
	for _, t := range dsw.batchGraph {
		builder.WriteString(t)
	}
	updateData.InsertTriples = builder.String()

	var updateStatement bytes.Buffer

	if dsw.isFullSync {
		t := template.Must(template.New("sparqlInsert").Parse(insertTemplateText))
		err := t.Execute(&updateStatement, updateData)
		if err != nil {
			dsw.logger.Error("Executing insert template failed", "dataset", dsw.dataset.name, "err", err)
			return cdl.Err(err, cdl.LayerErrorInternal)
		}
	} else {
		updateData.ToDeleteResources = strings.Join(dsw.batchResources, " ")
		t := template.Must(template.New("sparqlUpdate").Parse(updateTemplateText))
		err := t.Execute(&updateStatement, updateData)
		if err != nil {
			dsw.logger.Error("Executing update template failed", "dataset", dsw.dataset.name, "err", err)
			return cdl.Err(err, cdl.LayerErrorInternal)
		}
	}

	dsw.logger.Debug("Flushing batch", "dataset", dsw.dataset.name, "triples", len(dsw.batchGraph))

	// execute the update
	err := sendSparqlUpdate(dsw.dataset.store.config.SparqlUpdateEndpoint, updateStatement.String(), dsw.dataset.store.config.Auth, dsw.logger)
	if err != nil {
		dsw.logger.Error("SPARQL update failed", "dataset", dsw.dataset.name, "err", err)
		return cdl.Err(err, cdl.LayerErrorInternal)
	}

	return nil
}

// SPARQLResult represents the root of a SPARQL JSON result format.
type SPARQLResult struct {
	Results SPARQLResults `json:"results"`
}

// SPARQLResults encapsulates the "results" part of the SPARQL JSON response.
type SPARQLResults struct {
	Bindings []SPARQLBinding `json:"bindings"`
}

// SPARQLBinding represents individual bindings of variables in the SPARQL result set.
type SPARQLBinding map[string]SPARQLValue

// SPARQLValue holds the actual data for a binding, including its type and value.
type SPARQLValue struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

func doSparqlQuery(logger cdl.Logger, endpoint, query string, auth *SparqlEndpointAuth) (*SPARQLResult, error) {
	logger.Debug("Executing SPARQL query", "endpoint", endpoint, "query", query)

	// Prepare the HTTP request
	req, err := http.NewRequest("POST", endpoint, bytes.NewBufferString(query))
	if err != nil {
		logger.Error("Failed to create request", "endpoint", endpoint, "err", err)
		return nil, fmt.Errorf("creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/sparql-query")
	req.Header.Set("Accept", "application/sparql-results+json")
	if auth != nil && strings.ToLower(auth.Type) == "basic" && auth.User != "" {
		req.SetBasicAuth(auth.User, auth.Secret)
	}

	// Send the request
	resp, err := insecureHTTPClient.Do(req)
	if err != nil {
		logger.Error("Request failed", "endpoint", endpoint, "err", err)
		return nil, fmt.Errorf("sending request: %v", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("Reading response body failed", "endpoint", endpoint, "err", err)
		return nil, fmt.Errorf("reading response body: %v", err)
	}

	// Parse the JSON response
	var result SPARQLResult
	if err := json.Unmarshal(body, &result); err != nil {
		logger.Error("Parsing JSON failed", "endpoint", endpoint, "err", err)
		return nil, fmt.Errorf("parsing JSON: %v", err)
	}

	// Return the number of rows in the result
	return &result, nil
}

func sendSparqlUpdate(endpoint, updateQuery string, auth *SparqlEndpointAuth, logger cdl.Logger) error {
	// Prepare the HTTP request with the SPARQL update query as the body
	req, err := http.NewRequest("POST", endpoint, bytes.NewBufferString(updateQuery))
	if err != nil {
		logger.Error("Failed to create update request", "endpoint", endpoint, "err", err)
		return fmt.Errorf("could not create request: %v", err)
	}
	logger.Debug("SparqlDatasetWriter: writing triples", "endpoint", endpoint, "query", updateQuery)

	// Set appropriate headers
	req.Header.Set("Content-Type", "application/sparql-update")
	req.Header.Set("Accept", "application/sparql-results+json")
	if auth != nil && strings.ToLower(auth.Type) == "basic" && auth.User != "" {
		req.SetBasicAuth(auth.User, auth.Secret)
	}

	// Create an HTTP client and send the request
	resp, err := insecureHTTPClient.Do(req)
	if err != nil {
		logger.Error("Failed to send update request", "endpoint", endpoint, "err", err)
		return fmt.Errorf("could not send request: %v", err)
	}
	defer resp.Body.Close()

	// Check the response status
	if !(resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent) {
		body, _ := io.ReadAll(resp.Body)
		logger.Error("SPARQL update failed", "endpoint", endpoint, "status", resp.Status, "body", string(body))
		return fmt.Errorf("SPARQL update failed: %s %s", resp.Status, string(body))
	}

	return nil
}

type UpdateData struct {
	Graph             string
	InsertTriples     string
	ToDeleteResources string
}

const updateTemplateText = `
DELETE {
    GRAPH <{{ .Graph }}> {
        ?subject ?predicate ?object
    }
}
INSERT {
    GRAPH <{{ .Graph }}> {
        {{ .InsertTriples }}
    }
}
WHERE {
    VALUES ?subject { {{ .ToDeleteResources }} }
    BIND(NOW() AS ?now)
    OPTIONAL {
        GRAPH <{{ .Graph }}> {
            ?subject ?predicate ?object
        }
    }
}`

const insertTemplateText = `
INSERT {
	GRAPH <{{ .Graph }}> {
		{{ .InsertTriples }}
	}
}
WHERE {
    BIND (NOW() AS ?now)
}`

func (dsw *SparqlDatasetWriter) Close() cdl.LayerError {
	if dsw.written > 0 {
		dsw.logger.Debug("Closing writer, flushing remaining entities", "dataset", dsw.dataset.name, "remaining", dsw.written)
		err := dsw.Flush()
		if err != nil {
			dsw.logger.Error("Final flush failed", "dataset", dsw.dataset.name, "err", err)
			return err
		}
	}

	// if full sync and last batch, we need to copy the data from the temp graph to the main graph
	if dsw.isFullSync && dsw.fullSyncStrategy == TmpGraph && dsw.batchInfo.IsLastBatch {
		dsw.logger.Debug("Moving temp graph to main graph", "dataset", dsw.dataset.name)
		moveGraphQuery := fmt.Sprintf("MOVE GRAPH <%s> TO <%s>", dsw.fullSyncTempGraph, dsw.dataset.config.Graph)
		err := sendSparqlUpdate(dsw.dataset.store.config.SparqlUpdateEndpoint, moveGraphQuery, dsw.dataset.store.config.Auth, dsw.logger)
		if err != nil {
			dsw.logger.Error("Moving temp graph failed", "dataset", dsw.dataset.name, "err", err)
			return cdl.Err(err, cdl.LayerErrorInternal)
		}
	}

	dsw.logger.Info("Closed dataset writer", "dataset", dsw.dataset.name, "written", dsw.written)
	return nil
}
