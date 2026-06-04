package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lovoo/goka"
	"HW2/internal/censor"
	"HW2/internal/gokahelper"
	"HW2/internal/message"
	"HW2/internal/userblock"
	"HW2/internal/wordblock"
)

var (
	usersLookup *goka.View
	wordsLookup *goka.View
)

func main() {
	action := flag.String("action", "", "send, inbox or consume")
	from := flag.String("from", "", "sender")
	to := flag.String("to", "", "recipient")
	text := flag.String("text", "", "message text")
	flag.Parse()

	switch *action {
	case "send":
		handleSend(*from, *to, *text) 
	case "consume":
		handleConsume()
	default:
		fmt.Println("Usage: -action [send|consume] -from SENDER -to RECIPIENT [-text MESSAGE]")
		fmt.Println("Start messages, users and words consume before send.")
	}
}

func handleSend(from, to, text string) {
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	text = strings.TrimSpace(text)
	if from == "" || to == "" || text == "" {
		fmt.Println("from, to and text cannot be empty")
		return
	}
	if from == to {
		fmt.Println("cannot send message to yourself")
		return
	}

	key := messageKey(to, from, time.Now().UnixNano())
	if err := gokahelper.Emit(goka.Stream(message.Stream), message.Codec, key, message.Message{From: from, To: to, Text: text}); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("✓ Message queued from %q to %q\n", from, to)
}

func messageKey(to, from string, id int64) string {
	return to + userblock.KeySep + from + userblock.KeySep + strconv.FormatInt(id, 10)
}

func handleConsume() {
	var err error
	var cancelUsers, cancelWords context.CancelFunc

	usersLookup, cancelUsers, err = gokahelper.StartLookupTable(goka.Table(userblock.Table), userblock.Codec)
	if err != nil {
		fmt.Printf("Error starting users lookup: %v\n", err)
		return
	}
	defer cancelUsers()

	wordsLookup, cancelWords, err = gokahelper.StartLookupTable(goka.Table(wordblock.Table), wordblock.Codec)
	if err != nil {
		fmt.Printf("Error starting words lookup: %v\n", err)
		return
	}
	defer cancelWords()

	// Топик таблицы filtered_messages (без суффикса -table).
	goka.SetTableSuffix("")
	defer goka.ResetSuffixes()

	g := goka.DefineGroup(goka.Group(message.Group),
		goka.Input(goka.Stream(message.Stream), message.Codec, processMessage),
		goka.Persist(message.Codec),
	)

	fmt.Println("Consumer started, waiting for events...")
	if err := gokahelper.RunProcessorWithTable(goka.Stream(message.Stream), goka.Table(message.Table), g); err != nil {
		fmt.Printf("Processor error: %v\n", err)
	}
}

func processMessage(ctx goka.Context, msg interface{}) {
	m := msg.(message.Message)

	if gokahelper.ViewHasKey(usersLookup, userblock.BlockKey(m.To, m.From)) {
		fmt.Printf("[BLOCKED] %q blocked %q — message not delivered\n", m.To, m.From)
		return
	}

	original := m.Text
	m.Text = censor.Filter(m.Text, func(word string) bool {
		return censor.ViewHasWord(wordsLookup, word)
	})
	if m.Text != original {
		fmt.Printf("[CENSORED] %q -> %q\n", original, m.Text)
	}

	ctx.SetValue(m)
	fmt.Printf("[DELIVERED] %q -> %q: %s\n", m.From, m.To, m.Text)
}
