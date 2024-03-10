package base

import (
	"errors"

	"github.com/rmravindran/boostdb/stdlib"
)

type ColumnNameExpression struct {
	name      string
	sqlOpType SqlOpType
}

type ColumnNameExpressionState struct {
	name  string
	value any
}

// Create a new column name expression
func NewColumnNameExpression(name string, sqlOpType SqlOpType) *ColumnNameExpression {
	return &ColumnNameExpression{name: name, sqlOpType: sqlOpType}
}

// Return the type of the sql expression that generated this expression
func (cne *ColumnNameExpression) SqlOp() SqlOpType {
	return cne.sqlOpType
}

// Evaluate the non-const literal expression and produces a const literal
// expression with the specified int64 typed args argument. If the expression
// is already a const, evaluate with a nil parameter to produce a const
// expression containing the same value as this one. Evaluation by any argument
// other than nil on a const expression, results in error
func (cne *ColumnNameExpression) Evaluate(args interface{}) *stdlib.MaybeOp[Expression] {
	if args == nil {
		return stdlib.ErrorOp[Expression](errors.New("cannot evaluate column name expression without values"))
	}
	switch v := args.(type) {
	case int64:
		return stdlib.JustOp[Expression](
			NewLiteralIntConstExpression(cne.name, v))
	case float64:
		return stdlib.JustOp[Expression](
			NewLiteralFloatConstExpression(cne.name, v))
	case string:
		return stdlib.JustOp[Expression](
			NewLiteralStringConstExpression(cne.name, v))
	case bool:
		return stdlib.JustOp[Expression](
			NewLiteralBoolConstExpression(cne.name, v))
	}

	return stdlib.ErrorOp[Expression](errors.New("invalid value type"))
}

// Return the final values produced by the operator, otherwise returns nil
func (cne *ColumnNameExpression) Value() any {
	return cne
}

// Return false to indicate that column name expressions are not constant
func (cne *ColumnNameExpression) IsConstant() bool {
	return false
}

// Prepare an expression
func (cne *ColumnNameExpression) Prepare(
	nameHandler ArgNameHandler) ExpressionState {

	if nameHandler != nil {
		nameHandler(cne.name)
	}

	return &ColumnNameExpressionState{name: cne.name, value: nil}
}

// Returns the current int64 value of the state if the expression state is
// non-const, otherwise returns nil
func (cnes *ColumnNameExpressionState) ToArgs() interface{} {

	return cnes.value
}

// Set the value of the expression
func (cnes *ColumnNameExpressionState) SetValue(name string, value any) error {
	// Do nothing if the name does not match
	if name != cnes.name {
		return nil
	}

	cnes.value = value

	return nil
}

// Returns false to indicate that column ame expressions are not constant
func (cnes *ColumnNameExpressionState) IsConstant() bool {
	return false
}
