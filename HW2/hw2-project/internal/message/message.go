package message

import "HW2/internal/codec"

const (
	// Stream — входящие сообщения (до фильтрации).
	Stream = "messages"
	// Group — processor group; compact-топик таблицы = Table.
	Group = "filtered_messages"
	// Table — доставленные и отфильтрованные сообщения.
	Table = "filtered_messages"
)

type Message struct {
	From string
	To   string
	Text string
}

var Codec = &codec.JSON[Message]{}
