package base

import "errors"

type ResultSet struct {
	data [][]any

	maxRows int
	maxCols int
	rows    int
	cols    int
}

// Create an instance of ResultSet

func NewResultSet(
	maxRows int) *ResultSet {

	ret := &ResultSet{
		maxRows: maxRows,
		maxCols: 0,
		rows:    0,
		cols:    0}

	// TODO: Get this from arena
	ret.data = make([][]any, maxRows)
	return ret
}

func (rs *ResultSet) Clear() {
	rs.rows = 0
	rs.cols = 0
}

func (rs *ResultSet) Set(r int, c int, value any) error {

	if r >= rs.rows || c >= rs.cols {
		return errors.New("index out of bound")
	}

	rs.data[r][c] = value
	return nil
}

func (rs *ResultSet) Get(r int, c int) (any, error) {
	if r >= rs.rows || c >= rs.cols {
		return nil, errors.New("index out of bound")
	}
	return rs.data[r][c], nil
}

func (rs *ResultSet) GetFloat(r int, c int) (float64, error) {
	if r >= rs.rows || c >= rs.cols {
		return 0, errors.New("index out of bound")
	}
	return rs.data[r][c].(float64), nil
}

func (rs *ResultSet) GetString(r int, c int) (string, error) {
	if r >= rs.rows || c >= rs.cols {
		return "", errors.New("index out of bound")
	}
	return rs.data[r][c].(string), nil
}

func (rs *ResultSet) Resize(rows int) {
	if rows <= rs.maxRows {
		rs.rows = rows
	}
}

func (rs *ResultSet) ReDim(cols int) {
	if len(rs.data[0]) < cols {
		for r := range rs.data {
			rs.data[r] = make([]any, cols)
		}
		rs.maxCols = cols
	}
	rs.cols = cols
}

func (rs *ResultSet) NumRows() int {
	return rs.rows
}

func (rs *ResultSet) NumCols() int {
	return rs.cols
}
