package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/lovoo/goka"
	"HW2/internal/gokahelper"
	"HW2/internal/wordblock"
)

func main() {
	action := flag.String("action", "", "add, delete, list or consume")
	word := flag.String("word", "", "word to add or delete")
	flag.Parse()

	switch *action {
	case "add", "delete":
		emitWord(*action, *word)
	case "list":
		gokahelper.List(wordblock.Group, wordblock.Codec, "\n=== Blocked Words ===", "", func(v interface{}) (string, bool) {
			w, ok := v.(wordblock.BlockedWord)
			return w.Txt, ok
		})
	case "consume":
		fmt.Println("Consumer started, waiting for events...")
		if err := gokahelper.RunConsume(goka.Stream(wordblock.Stream), goka.Group(wordblock.Group), wordblock.Codec, processWord); err != nil {
			fmt.Printf("Processor error: %v\n", err)
		}
	default:
		fmt.Println("Usage: -action [add|delete|list|consume] [-word WORD]")
		fmt.Println("Start consume before add/delete/list.")
	}
}

func emitWord(action, word string) {
	word = strings.TrimSpace(word)
	if word == "" {
		fmt.Println("Word cannot be empty")
		return
	}
	if err := gokahelper.Emit(goka.Stream(wordblock.Stream), wordblock.Codec, word, wordblock.BlockedWord{Txt: word, Action: action}); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("✓ Word %q (%s)\n", word, action)
}

func processWord(ctx goka.Context, msg interface{}) {
	word := msg.(wordblock.BlockedWord)

	switch word.Action {
	case "add":
		ctx.SetValue(word)
		fmt.Printf("[ADD] %s\n", word.Txt)
	case "delete":
		ctx.Delete()
		fmt.Printf("[DELETE] %s\n", word.Txt)
	default:
		fmt.Printf("[SKIP] unknown action %q for %s\n", word.Action, word.Txt)
	}
}
