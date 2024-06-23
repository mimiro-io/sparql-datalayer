package main

import (
	common "github.com/mimiro-io/common-datalayer"
	layer "github.com/mimiro-io/sparql-datalayer"
	"os"
)

func main() {
	configFolderLocation := ""
	args := os.Args[1:]
	if len(args) >= 1 {
		configFolderLocation = args[0]
	}
	common.NewServiceRunner(layer.NewSparqlDataLayer).WithConfigLocation(configFolderLocation).StartAndWait()
}
