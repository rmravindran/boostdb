package base

type QueryOps struct {
}

// Create a new QueryOps
func NewQueryOps() *QueryOps {
	return &QueryOps{}
}

// Add fetch operations to the set of query operations
func (qo *QueryOps) AddSourceFetchOp(
	source string,
	alias string) {
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
	fieldName string,
	source string) {
}

// Adds the WHERE expression. This is the root of the where expression.
func (qo *QueryOps) AddWhereExpression(
//rootExpression *WhereExpression
) {

}
