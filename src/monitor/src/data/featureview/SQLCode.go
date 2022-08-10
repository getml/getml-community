package featureview

// --------------------------------------------------------

import (
	"monitor/data"
	"net/http"
)

// --------------------------------------------------------

// SQLCode contains the code to be displayed.
type SQLCode struct {
	SQL string
}

// --------------------------------------------------------

// NewSQLCode is a constructor.
func NewSQLCode(
	pipelineName string,
	featureIx int,
	standardVars data.StandardVariables) *SQLCode {

	// We do not need to explicitly pass the feature name,
	// if anything that would make things slower.
	sql, err := loadSQL(pipelineName, "", featureIx, standardVars)

	if err != nil {
		return NewEmptySQLCode()
	}

	data := NewEmptySQLCode()

	data.SQL = overwriteOversizedFeatures(sql)

	return data

}

// --------------------------------------------------------

// NewEmptySQLCode is a constructors
func NewEmptySQLCode() *SQLCode {

	return &SQLCode{SQL: ""}
}

// --------------------------------------------------------

// Write writes the results into the writer.
func (s *SQLCode) Write(w http.ResponseWriter) {
	w.Write([]byte(s.SQL))
}

// --------------------------------------------------------
