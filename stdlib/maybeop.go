package stdlib

// Defines an interface for an evaluatable object
type Op[T any] interface {

	// Evaluates this object and returns the result operator that can either
	// represent the resulting values of this operation or be curried further.
	Evaluate(args interface{}) *MaybeOp[T]

	// Return the final value produced by the operator, otherwise returns nil
	Value() any
}

// ----------------------------------------------------------------------------
// - MaybeOp Struct
// ----------------------------------------------------------------------------

// Provides functionality similar to the Maybe monad in Haskell. This is used
// to represent the result of an operation that can either be an error or a
// valid operation.
type MaybeOp[T any] struct {
	op  Op[T]
	err error
}

// --------------
// - CONSTRUCTORS
// --------------

// Create a new MaybeOp that represents a valid operation.
func JustOp[T any](op Op[T]) *MaybeOp[T] {
	return &MaybeOp[T]{op: op, err: nil}
}

// Create a new MaybeOp that represents an error.
func ErrorOp[T any](err error) *MaybeOp[T] {
	return &MaybeOp[T]{op: nil, err: err}
}

// ----------------
// - PUBLIC METHODS
// ----------------

// Return the error if the operation was a failure, otherwise returns nil
func (op *MaybeOp[T]) Error() error {
	return op.err
}

// Return the underlying operator
func (op *MaybeOp[T]) Op() Op[T] {
	return op.op
}

// Evalue the operator on the specified args and return a new operator that can
// either represent the resulting values of this operation or be further
// curried.
func (op *MaybeOp[T]) Evaluate(args interface{}) *MaybeOp[T] {

	if op.Error() != nil {
		return op
	}

	return op.op.Evaluate(args)
}

// Return the final values produced by the operator, otherwise returns nil
func (op *MaybeOp[T]) Value() any {
	if op.Error() != nil {
		return nil
	}
	return op.op.Value()
}
