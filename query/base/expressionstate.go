package base

// Expression state contains a state representation of the expression. The
// state is used house expression specific state information. The base type
// described here provides a common interface for all expression states.
type ExpressionState interface {

	// Returns an array of data that represents an initial state. These
	// can be passed into arguments that can be used to evaluate an
	// expression
	InitState() interface{}
}
