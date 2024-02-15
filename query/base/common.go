package base

// Constant Value Types
type ValueType int

const (
	// Parsed as an integer
	ValueTypeInt ValueType = iota

	// Parsed as a string
	ValueTypeString

	// Parsed as a float
	ValueTypeFloat

	// Parsed boolean
	ValueTypeBool
)

// Constant Types
type ConstTypes interface {
	bool | int64 | float64 | string
}
