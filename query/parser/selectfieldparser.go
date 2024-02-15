package parser

import (
	"errors"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// Select Field Parser state type
type SelectFieldsParseState int

const (
	// Parsing None
	ParsingSelectFieldNone SelectFieldsParseState = iota

	// Parsing the root node that holds all the Fields
	ParsingSelectFieldsNode

	// Parsing a specific Field
	ParsingSelectField

	// Parsing a column name expression node
	ParsingSelectColNameExprNode

	// Parsing a column name node
	ParsingSelectColNameNode
)

type SelectFieldInfo struct {
	FieldName string
	Source    string
}

type SelectFieldsVisitor struct {
	CurrentParseState SelectFieldsParseState

	// Is state invalid
	IsInvalid bool

	// Error message
	Error error

	// Parsing text
	ParsingText string

	// All the fields in the select statement
	Fields []*SelectFieldInfo

	// Field Name and Source
	currentFieldInfo *SelectFieldInfo
}

// Create a new select fields visitor
func NewSelectFieldsVisitor() *SelectFieldsVisitor {
	return &SelectFieldsVisitor{
		CurrentParseState: ParsingSelectFieldNone,
		Error:             nil,
		IsInvalid:         false,
		Fields:            []*SelectFieldInfo{},
	}
}

func (v *SelectFieldsVisitor) Enter(in ast.Node) (ast.Node, bool) {
	skipChildren := false

	switch n := in.(type) {
	case *ast.FieldList:
		v.CurrentParseState = ParsingSelectFieldsNode
		// Do nothing
	case *ast.SelectField:
		v.ParsingText = n.Text()
		if v.CurrentParseState != ParsingSelectFieldsNode {
			if v.Error == nil {
				v.Error = errors.New("select field at '" + v.ParsingText + "'")
			}
			// Query structure is not as we expected.
			v.IsInvalid = true
			skipChildren = true
		} else {
			v.CurrentParseState = ParsingSelectField
			v.currentFieldInfo = &SelectFieldInfo{}
		}
	case *ast.ColumnNameExpr:
		if v.CurrentParseState != ParsingSelectField {
			if v.Error == nil {
				v.Error = errors.New("select field at '" + v.ParsingText + "'")
			}
			v.IsInvalid = true
			skipChildren = true
		} else {
			v.CurrentParseState = ParsingSelectColNameExprNode
		}
	case *ast.ColumnName:
		if v.CurrentParseState != ParsingSelectColNameExprNode {
			if v.Error == nil {
				v.Error = errors.New("select field at '" + v.ParsingText + "'")
			}
			v.IsInvalid = true
			skipChildren = true
		} else {
			if n.Name.O == "" {
				if v.Error == nil {
					v.Error = errors.New("select field at '" + v.ParsingText + "'")
				}
				v.IsInvalid = true
				skipChildren = true
			} else {
				v.currentFieldInfo.FieldName = n.Name.O
				v.currentFieldInfo.Source = n.Table.O
				v.CurrentParseState = ParsingSelectColNameNode
			}
		}
	}

	return in, skipChildren
}

func (v *SelectFieldsVisitor) Leave(in ast.Node) (ast.Node, bool) {
	switch in.(type) {
	case *ast.SelectField:
		// If we didn't see a column name node, then the state transitions
		// didn't end where we hoped it to end.
		if v.CurrentParseState != ParsingSelectColNameNode {
			v.IsInvalid = true
			if v.Error == nil {
				v.Error = errors.New("select field at '" + v.ParsingText + "'")
			}
		} else if !v.IsInvalid {
			v.Fields = append(v.Fields, v.currentFieldInfo)
			v.currentFieldInfo = nil
		}
		v.CurrentParseState = ParsingSelectFieldsNode
	}
	return in, true
}
