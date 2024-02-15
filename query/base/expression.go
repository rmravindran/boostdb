package base

// There are the following types of sql operations allowed
type SqlOpType int

const (
	// A literal is a fixed value such as a number, a string, or a datetime
	// value. This includes NULL, which represents a missing or unknown value.
	Literal SqlOpType = iota

	// A Select is an expression that fetches an attribute/tag from a table.
	Select

	// An Aggregate is a subquery that returns a single value, usually from
	// multiple rows of a table.
	Aggregate

	// Where is an expression that filters the result set based on a condition.
	Where

	// GroupBy is an expression that groups the result set by one or more
	// columns.
	GroupBy

	// Having is an expression that filters the result set based on a condition.
	Having

	// OrderBy is an expression that sorts the result set by one or more
	// columns.
	OrderBy
)

// An expression is a evaluatable object that may return a Maybe[Expression]
// upon evaluation. Note that a constant is also an evaluatable object, which
// evaluates to itself that contains the same value.
type Expression interface {

	// The type of the sql expression that generated this expression
	SqlOp() SqlOpType

	// Is the expression a constant
	IsConstant() bool

	// Prepare an expression
	Prepare() ExpressionState
}

type NullArgs struct {
}

// Create a constant null argument
func NewNullArgs() NullArgs {
	return NullArgs{}
}
