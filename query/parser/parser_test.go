package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Unit test the parser
func TestParser_SimpleSelect(t *testing.T) {
	p := NewParser()
	queryOps, err := p.Parse("select a, b from j.s testAlias")
	require.Nil(t, err)
	require.NotNil(t, queryOps)

	// Expecting 2 select field operations
	selectFieldOps := queryOps.SelectFieldOps()
	require.Equal(t, 2, len(selectFieldOps))
	// Check the first select field operation
	require.Equal(t, "value", selectFieldOps[0].AttributeName)
	require.Equal(t, "a", selectFieldOps[0].SeriesName)
	require.True(t, selectFieldOps[0].SourceAlias == "" || selectFieldOps[0].SourceAlias == "s")
	// Check the second select field operation
	require.Equal(t, "value", selectFieldOps[1].AttributeName)
	require.Equal(t, "b", selectFieldOps[1].SeriesName)
	require.True(t, selectFieldOps[1].SourceAlias == "" || selectFieldOps[1].SourceAlias == "s")

	// Expecting 1 source fetch operation
	sourceFetchOps := queryOps.SourceFetchOps()
	require.Equal(t, 1, len(sourceFetchOps))
	// Check the source fetch operation
	require.Equal(t, "s", sourceFetchOps[0].Source)
	require.Equal(t, "j", sourceFetchOps[0].Domain)
	require.Equal(t, "testAlias", sourceFetchOps[0].Alias)
}

// Unit test the parser
func TestParser_SimpleWhere(t *testing.T) {
	p := NewParser()
	queryOps, err := p.Parse("SELECT a FROM s WHERE a < 10")
	require.Nil(t, err)
	require.NotNil(t, queryOps)

	selectFieldOps := queryOps.SelectFieldOps()
	require.Equal(t, 1, len(selectFieldOps))

	whereExp := queryOps.WhereExpression()
	require.NotNil(t, whereExp)
}

func TestParser_JoinAndWhere(t *testing.T) {
	p := NewParser()
	queryOps, err := p.Parse("select a, t.b from s INNER JOIN t ON s.x = t.x where a < 10 and s.y = t.y")
	require.Nil(t, err)
	require.NotNil(t, queryOps)

	selectFieldOps := queryOps.SelectFieldOps()
	require.Equal(t, 2, len(selectFieldOps))
}
