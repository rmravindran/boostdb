package base

import "github.com/rmravindran/boostdb/stdlib"

// QueryOps represents a collection of operations that could be performed to
// generate a result set. PLanner will use the queryOps to generate an
// execution plan

type QueryOps struct {

	// List of all source fetch operations
	sourceFetchOps []SourceFetchOp

	// List of all select field operations
	selectFieldOps []SelectFieldOp
}

type SourceFetchOp struct {
	Domain        string
	Source        string
	QualifiedName string
	Alias         string
}

type SelectFieldOp struct {
	SourceAlias   string
	SeriesName    string
	AttributeName string
}

// Create a new QueryOps
func NewQueryOps() *QueryOps {
	return &QueryOps{}
}

// Add fetch operations to the set of query operations
func (qo *QueryOps) AddSourceFetchOp(
	domain string,
	source string,
	alias string) {

	qualifedName := source
	if domain != "" {
		qualifedName = domain + "." + source
	}

	if alias == "" {
		alias = qualifedName
	}
	qo.sourceFetchOps = append(qo.sourceFetchOps, SourceFetchOp{domain, source, qualifedName, alias})
}

// Adds a Join Operation to the set of query operations
func (qo *QueryOps) AddJoinOp(
	leftSource string,
	leftColumn string,
	rightSource string,
	rightColumn string) {
}

// Adds a Select Field Operation to the set of query operations
func (qo *QueryOps) AddSelectFieldOp(
	alias string,
	seriesName string,
	attributeName string) {
	qo.selectFieldOps = append(qo.selectFieldOps, SelectFieldOp{alias, seriesName, attributeName})
}

// Adds the WHERE expression. This is the root of the where expression.
func (qo *QueryOps) AddWhereExpression(
	rootExpression *stdlib.MaybeOp[Expression]) {
}

// Return the source fetch operations
func (qo *QueryOps) SourceFetchOps() []SourceFetchOp {
	return qo.sourceFetchOps
}

// Return the select field operations
func (qo *QueryOps) SelectFieldOps() []SelectFieldOp {
	return qo.selectFieldOps
}
