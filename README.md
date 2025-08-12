# Universal Data API - SPARQL Data Layer

UDA compliant SPARQL data layer

## Starting the SPARQL DataLayer

Map the host folder containing the config as a volume and then provide the mapped volume as the first argument.

```sh
docker run -v {PWD}/config:/root/config mimiro/sparql-datalayer /root/config
```

## Configuration

The following example config shows how to set up the data layer.

```json
{
  "layer_config": {
    "port": "8090",
    "service_name": "sparql_service",
    "log_level": "DEBUG",
    "log_format": "json",
    "config_refresh_interval": "200s"
  },
  "system_config" : {
    "sparql_query_endpoint" : "http://localhost:7200/repositories/test_layer",
    "sparql_update_endpoint" : "http://localhost:7200/repositories/test_layer/statements",
    "auth" : {
      "type" : "basic",
      "user" : "",
      "secret" : "",
      "endpoint" : ""
    }
  },
  "dataset_definitions" : [
    {
      "name" : "people",
      "source_config" : {
        "graph" : "http://example.org/people",
        "last_modified_predicate" : "http://purl.org/dc/terms/modified",
        "write_batch_size" : 1000,
        "full_sync_update_strategy" : "tmpgraph"
      }
    }
  ]
}
```

### Environment overrides

The SPARQL connection settings, including basic authentication, can also be
provided through environment variables:

- `SPARQL_QUERY_ENDPOINT`
- `SPARQL_UPDATE_ENDPOINT`
- `SPARQL_USER`
- `SPARQL_SECRET`

If these variables are set they will override the corresponding values in the
configuration file.

