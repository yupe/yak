package userblock

import "HW2/internal/codec"

const (
	KeySep = ":"
	Stream = "blocked_users"
	Group  = "blocked_users-group"
	Table  = "blocked_users-group-table"
)

type UserBlock struct {
	User        string
	BlockedUser string
	Action      string
}

var Codec = &codec.JSON[UserBlock]{}

func BlockKey(user, blockedUser string) string {
	return user + KeySep + blockedUser
}
