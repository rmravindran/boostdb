package base

import (
	"errors"

	"github.com/rmravindran/boostdb/stdlib"
)

type LiteralFloatExpression struct {
	name       string
	constValue float64
	isConst    bool
}

type LiteralFloatExpressionState struct {
	name    string
	value   float64
	isConst bool
}

// Create a new literal float expression
func NewLiteralFloatExpression(name string) *LiteralFloatExpression {
	return &LiteralFloatExpression{name: name, constValue: 0.0, isConst: false}
}

// Create a new literal float expression with a constant value
func NewLiteralFloatConstExpression(
	name string, value float64) *LiteralFloatExpression {
	return &LiteralFloatExpression{
		name: name, constValue: value, isConst: true}
}

// Return the type of the sql expression that generated this expression
func (le *LiteralFloatExpression) SqlOp() SqlOpType {
	return Literal
}

// Evaluate the non-const literal expression and produces a const literal
// expression with the specified float64 typed args argument. If the expression
// is already a const, evaluate with a nil parameter to produce a const
// expression containing the same value as this one. Evaluation by any argument
// other than nil on a const expression, results in error
func (le *LiteralFloatExpression) Evaluate(args interface{}) *stdlib.MaybeOp[Expression] {
	if args != nil {
		if le.isConst {
			return stdlib.ErrorOp[Expression](errors.New("cannot evaluate a constant literal expression with arguments"))
		}

		// Return a new expression with the constant value set
		return stdlib.JustOp[Expression](
			NewLiteralFloatConstExpression(le.name, args.(float64)))
	}
	return stdlib.JustOp[Expression](le)
}

// Return the final values produced by the operator, otherwise returns nil
func (le *LiteralFloatExpression) Value() any {
	return le
}

// Return the float value
func (le *LiteralFloatExpression) Float() float64 {
	return le.constValue
}

// Return true to indicate that this is a constant
func (le *LiteralFloatExpression) IsConstant() bool {
	return le.isConst
}

// Returns the current float64 value of the state if the expression state is
// non-const, otherwise returns nil
func (le *LiteralFloatExpression) ToArgs() interface{} {
	if le.isConst {
		return nil
	}

	return le.constValue
}

// Prepare an expression
func (le *LiteralFloatExpression) Prepare(
	nameHandler ArgNameHandler) ExpressionState {

	if !le.isConst && nameHandler != nil {
		nameHandler(le.name)
	}

	return &LiteralFloatExpressionState{
		name:    le.name,
		value:   le.constValue,
		isConst: le.isConst,
	}
}

// Return the Expression Arg compatible representation of the state
func (les *LiteralFloatExpressionState) ToArgs() interface{} {
	if les.isConst {
		return nil
	}

	return les.value
}

// Set the value of the expression
func (les *LiteralFloatExpressionState) SetValue(name string, value any) error {
	if name != les.name {
		return nil
	}

	if les.isConst {
		return errors.New("cannot set the value of a constant literal expression")
	}

	switch v := value.(type) {
	case float64:
		les.value = v
		return nil
	}

	return errors.New("invalid value type")
}

// Return true if the expression is a constant, otherwise returns false
func (les *LiteralFloatExpressionState) IsConstant() bool {
	return les.isConst
}
