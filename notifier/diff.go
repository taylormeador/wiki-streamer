package main

// DiffLineType is the wikidiff2 inline-diff line classification.
// 0 = unchanged, 1 = added, 2 = removed, 3 = changed (merged old+new text,
// with HighlightRanges marking which spans within it are which).
type DiffLineType int

const (
	DiffContext DiffLineType = 0
	DiffAdded   DiffLineType = 1
	DiffRemoved DiffLineType = 2
	DiffChanged DiffLineType = 3
)

// HighlightRangeType marks a span within a DiffChanged line's Text.
const (
	HighlightRemoved = 0
	HighlightAdded   = 1
)

type CompareResponse struct {
	Diff []DiffLine `json:"diff"`
}

type DiffLine struct {
	Type            DiffLineType     `json:"type"`
	LineNumber      int              `json:"lineNumber"`
	Text            string           `json:"text"`
	Offset          DiffOffset       `json:"offset"`
	HighlightRanges []HighlightRange `json:"highlightRanges,omitempty"`
}

// From/To are nil exactly when the line doesn't exist on that side
// (From nil => DiffAdded, To nil => DiffRemoved).
type DiffOffset struct {
	From *int `json:"from"`
	To   *int `json:"to"`
}

type HighlightRange struct {
	Start  int `json:"start"`
	Length int `json:"length"`
	Type   int `json:"type"`
}
