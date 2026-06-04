package wordblock

import "HW2/internal/codec"

const (
	Stream = "blocked-words"
	Group  = "blocked-words-group"
	Table  = "blocked-words-group-table"
)

type BlockedWord struct {
	Txt    string
	Action string
}

var Codec = &codec.JSON[BlockedWord]{}
