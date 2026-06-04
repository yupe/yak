package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/lovoo/goka"
	"HW2/internal/gokahelper"
	"HW2/internal/userblock"
)

func main() {
	action := flag.String("action", "", "block, unblock, list or consume")
	user := flag.String("user", "", "user who blocks or unblocks")
	blockedUser := flag.String("blocked_user", "", "user to block or unblock")
	flag.Parse()

	switch *action {
	case "block", "unblock":
		emitUserBlock(*action, *user, *blockedUser)
	case "list":
		u := strings.TrimSpace(*user)
		if u == "" {
			fmt.Println("user cannot be empty")
			return
		}
		gokahelper.List(userblock.Group, userblock.Codec,
			fmt.Sprintf("\n=== Blocked by %s ===", u),
			u+userblock.KeySep,
			func(v interface{}) (string, bool) {
				b, ok := v.(userblock.UserBlock)
				return b.BlockedUser, ok
			})
	case "consume":
		fmt.Println("Consumer started, waiting for events...")
		if err := gokahelper.RunConsume(goka.Stream(userblock.Stream), goka.Group(userblock.Group), userblock.Codec, processBlock); err != nil {
			fmt.Printf("Processor error: %v\n", err)
		}
	default:
		fmt.Println("Usage: -action [block|unblock|list|consume] -user USER [-blocked_user BLOCKED_USER]")
		fmt.Println("Start consume before block/unblock/list.")
	}
}

func emitUserBlock(action, user, blockedUser string) {
	user = strings.TrimSpace(user)
	blockedUser = strings.TrimSpace(blockedUser)
	if user == "" || blockedUser == "" {
		fmt.Println("user and blocked_user cannot be empty")
		return
	}
	if action == "block" && user == blockedUser {
		fmt.Println("user cannot block themselves")
		return
	}

	err := gokahelper.Emit(goka.Stream(userblock.Stream), userblock.Codec,
		userblock.BlockKey(user, blockedUser),
		userblock.UserBlock{User: user, BlockedUser: blockedUser, Action: action})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("✓ User %q %s %q\n", user, action, blockedUser)
}

func processBlock(ctx goka.Context, msg interface{}) {
	block := msg.(userblock.UserBlock)

	switch block.Action {
	case "block":
		ctx.SetValue(block)
		fmt.Printf("[BLOCK] %s -> %s\n", block.User, block.BlockedUser)
	case "unblock":
		ctx.Delete()
		fmt.Printf("[UNBLOCK] %s -> %s\n", block.User, block.BlockedUser)
	default:
		fmt.Printf("[SKIP] unknown action %q for %s -> %s\n", block.Action, block.User, block.BlockedUser)
	}
}
