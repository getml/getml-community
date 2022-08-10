package featureview

import (
	"monitor/data"
	"monitor/data/helpers"
	"net/http"
)

// --------------------------------------------------------

// FeatureData contains the information for the feature view.
type FeatureData struct {
	Dependencies []Dependencies

	Features []string

	Targets []string
}

// --------------------------------------------------------

// NewFeatureData is a constructor.
func NewFeatureData(
	name string,
	standardVars data.StandardVariables) *FeatureData {

	pipe, found := helpers.GetPipeline(name, standardVars)

	if !found {
		return NewEmptyFeatureData()
	}

	dependencies, err := loadDependencies(name, standardVars)

	if err != nil {
		return NewEmptyFeatureData()
	}

	f := NewEmptyFeatureData()

	f.Dependencies = dependencies

	f.Features = pipe.Scores.FeatureNames

	f.Targets = pipe.Targets

	return f

}

// --------------------------------------------------------

// NewEmptyFeatureData is a constructor.
func NewEmptyFeatureData() *FeatureData {

	return &FeatureData{
		Dependencies: make([]Dependencies, 0),
		Features:     make([]string, 0),
		Targets:      make([]string, 0)}
}

// --------------------------------------------------------

// ToJSON transforms the struct to a JSON.
func (f *FeatureData) Write(w http.ResponseWriter) {
	helpers.Write(w, *f)
}

// --------------------------------------------------------
