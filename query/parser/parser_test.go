package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Unit test the parser
func TestParser_Parse(t *testing.T) {
	p := NewParser()
	queryOps, err := p.Parse("select s.a, t.b from s INNER JOIN t ON s.x = t.x where a < 10 and s.y = t.y")
	require.Nil(t, err)
	require.NotNil(t, queryOps)
}
