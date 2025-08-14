package layer

import (
	"bytes"
	"strings"
	"testing"
	"text/template"
)

func TestUpdateTemplateUsesGraph(t *testing.T) {
	data := UpdateData{
		Graph:             "http://example.org/people",
		InsertTriples:     "<s1> <p1> <o1> .",
		ToDeleteResources: "<s1>",
	}

	tplt := template.Must(template.New("sparqlUpdate").Parse(updateTemplateText))
	var buf bytes.Buffer
	if err := tplt.Execute(&buf, data); err != nil {
		t.Fatal(err)
	}
	result := buf.String()

	if strings.Contains(result, "WITH") {
		t.Errorf("expected no WITH clause, got: %s", result)
	}
	if count := strings.Count(result, "GRAPH <"+data.Graph+">"); count != 3 {
		t.Errorf("expected 3 GRAPH clauses, got %d: %s", count, result)
	}
}
