package base

// Expression state contains a state representation of the expression. The
// state is used house expression specific state information. The base type
// described here provides a common interface for all expression states.
type ExpressionState interface {

	// Returns an array of data that represents the current state. These
	// can be passed into arguments that can be used to evaluate an
	// expression
	ToArgs() interface{}

	// Set the value to the state having the specified state attribute name.
	// Returns an error if the value cannot be set, otherwise returns nil.
	SetValue(name string, value any) error

	// Returns true if the expression state is constant
	IsConstant() bool
}
