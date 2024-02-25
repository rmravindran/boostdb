package parser

import (
	"errors"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// Join Parser state type
type JoinParseState int

const (
	// Parsing None
	ParsingJoinNone JoinParseState = iota

	// Parsing Join Node
	ParsingJoinNode

	// Parsing sources
	ParsingJoinSources

	// Parsing Join Node
	ParsingJoinTableJoinNode

	// Parsing the ON Node
	ParsingJoinOnNode

	// Parsing Left Node
	ParsingJoinLeftNode

	// Parsing Right Node
	ParsingJoinRightNode
)

type JoinInfo struct {
	LeftSource  string
	LeftColumn  string
	RightSource string
	RightColumn string
}

type SourceInfo struct {
	SourceDomain string
	SourceName   string
	SourceType   string
	Alias        string
}

type JoinVisitor struct {
	CurrentJoinParseState JoinParseState

	// All sources
	Sources []*SourceInfo

	// All the joins
	Joins []*JoinInfo

	// Currently parsing join
	CurrentJoin *JoinInfo

	// Currently parsing source
	CurrentSource *SourceInfo

	// Is the state valid
	IsValid bool

	// Error
	Error error

	// Parsing text info
	ParsingText string
}

// Create a new join visitor
func NewJoinVisitor() *JoinVisitor {
	return &JoinVisitor{
		CurrentJoinParseState: ParsingJoinNone,
		Sources:               []*SourceInfo{},
		Joins:                 []*JoinInfo{},
		IsValid:               true,
	}
}

func (v *JoinVisitor) Enter(in ast.Node) (ast.Node, bool) {
	skipChildren := false
	switch n := in.(type) {
	case *ast.Join:
		// There could be many joins inside a join when the parser have
		// tokenized the FROM clause as a tree. The joins with just sources
		// are represented as a tree of joins (cross joins)

		if v.CurrentJoinParseState == ParsingJoinNone {
			// Root Join

			v.ParsingText = "from"
			v.CurrentJoinParseState = ParsingJoinNode
		}

		// Parse Left
		if n.Left != nil {
			n.Left.Accept(v)
		}

		// Parse Right
		if n.Right != nil && v.IsValid {
			n.Right.Accept(v)
		}

		// Now visit all the joins in the ON clause
		if n.On != nil && v.IsValid {
			v.ParsingText = "join"
			v.CurrentJoinParseState = ParsingJoinTableJoinNode
			n.On.Accept(v)
			v.CurrentJoinParseState = ParsingJoinNode
		}

		if !v.IsValid {
			v.Error = errors.New("at '" + v.ParsingText + "'")
		}
		skipChildren = true
	case *ast.TableSource:
		v.CurrentSource = &SourceInfo{}
		v.CurrentJoinParseState = ParsingJoinSources
		v.CurrentSource.SourceType = "table"
		v.CurrentSource.Alias = n.AsName.O
	case *ast.TableName:
		v.CurrentSource.SourceName = n.Name.O
		v.CurrentSource.SourceDomain = n.Schema.O
	case *ast.OnCondition:
		v.ParsingText = n.Text()
		if v.CurrentJoinParseState != ParsingJoinTableJoinNode {
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("on condition at '" + v.ParsingText + "'")
				skipChildren = true
			}
			break
		}
		v.CurrentJoinParseState = ParsingJoinOnNode
	case *ast.BinaryOperationExpr:
		if v.CurrentJoinParseState != ParsingJoinOnNode {
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("on condition at '" + v.ParsingText + "'")
				skipChildren = true
			}
			break
		}
		v.CurrentJoin = &JoinInfo{}
		v.CurrentJoinParseState = ParsingJoinLeftNode
		n.L.Accept(v)
		v.CurrentJoinParseState = ParsingJoinRightNode
		n.R.Accept(v)

		skipChildren = true
	case *ast.ColumnNameExpr:
		if v.CurrentJoinParseState != ParsingJoinLeftNode && v.CurrentJoinParseState != ParsingJoinRightNode {
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("on condition at '" + v.ParsingText + "'")
				skipChildren = true
			}
			break
		}
	case *ast.ColumnName:
		if v.CurrentJoinParseState == ParsingJoinLeftNode {
			v.CurrentJoin.LeftSource = n.Table.O
			v.CurrentJoin.LeftColumn = n.Name.O
		} else if v.CurrentJoinParseState == ParsingJoinRightNode {
			v.CurrentJoin.RightSource = n.Table.O
			v.CurrentJoin.RightColumn = n.Name.O
		} else {
			v.IsValid = false
			if v.Error == nil {
				v.Error = errors.New("on condition at '" + v.ParsingText + "'")
				skipChildren = true
			}
			break
		}
	}

	return in, skipChildren
}

func (v *JoinVisitor) Leave(in ast.Node) (ast.Node, bool) {
	switch in.(type) {
	case *ast.TableName:
		// Done with parsing the source
		if v.IsValid {
			v.Sources = append(v.Sources, v.CurrentSource)
			v.CurrentSource = nil
		}
	case *ast.BinaryOperationExpr:
		// Done with parsing an on condition
		if v.IsValid {
			v.Joins = append(v.Joins, v.CurrentJoin)
			v.CurrentJoin = nil
		}
	}
	return in, true
}
