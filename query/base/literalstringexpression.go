package base

import (
	"errors"

	"github.com/rmravindran/boostdb/stdlib"
)

type LiteralStringExpression struct {
	name       string
	constValue string
	isConst    bool
}

type LiteralStringExpressionState struct {
	name    string
	value   string
	isConst bool
}

// Create a new literal string expression
func NewLiteralStringExpression(name string) *LiteralStringExpression {
	return &LiteralStringExpression{name: name, constValue: "", isConst: false}
}

// Create a new literal string expression with a constant value
func NewLiteralStringConstExpression(
	name string, value string) *LiteralStringExpression {
	return &LiteralStringExpression{
		name: name, constValue: value, isConst: true}
}

// Return the type of the sql expression that generated this expression
func (le *LiteralStringExpression) SqlOp() SqlOpType {
	return Literal
}

// Evaluate the non-const literal expression and produces a const literal
// expression with the specified string typed args argument. If the expression
// is already a const, evaluate with a nil parameter to produce a const
// expression containing the same value as this one. Evaluation by any argument
// other than nil on a const expression, results in error
func (le *LiteralStringExpression) Evaluate(args interface{}) *stdlib.MaybeOp[Expression] {
	if args != nil {
		if le.isConst {
			return stdlib.ErrorOp[Expression](errors.New("cannot evaluate a constant literal expression with arguments"))
		}
		// Return a new expression with the constant value set
		return stdlib.JustOp[Expression](
			NewLiteralStringConstExpression(le.name, args.(string)))
	}
	return stdlib.JustOp[Expression](le)
}

// Return the final values produced by the operator, otherwise returns nil
func (le *LiteralStringExpression) Value() any {
	return le
}

// Return the string value
func (le *LiteralStringExpression) String() string {
	return le.constValue
}

// Return true to indicate that this is a constant
func (le *LiteralStringExpression) IsConstant() bool {
	return le.isConst
}

// Return the Initial State
func (le *LiteralStringExpression) ToArgs() interface{} {
	if le.isConst {
		return nil
	}

	return le.constValue
}

// Prepare an expression
func (le *LiteralStringExpression) Prepare(
	nameHandler ArgNameHandler) ExpressionState {

	if !le.isConst && nameHandler != nil {
		nameHandler(le.name)
	}

	return &LiteralStringExpressionState{
		name: le.name, value: le.constValue, isConst: le.isConst}
}

// Returns the current string value of the state if the expression state is
// non-const, otherwise returns nil
func (les *LiteralStringExpressionState) ToArgs() interface{} {
	if les.isConst {
		return nil
	}
	return les.value
}

// Set the value of the expression
func (les *LiteralStringExpressionState) SetValue(
	name string,
	value any) error {

	if name != les.name {
		return nil
	}

	if les.isConst {
		return errors.New("cannot set the value of a constant literal expression")
	}
	switch v := value.(type) {
	case string:
		les.value = v
		return nil
	}
	return errors.New("invalid value type")
}

// Return true if the expression is a constant, otherwise returns false
func (les *LiteralStringExpressionState) IsConstant() bool {
	return les.isConst
}
