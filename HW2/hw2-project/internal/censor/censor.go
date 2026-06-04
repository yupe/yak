package censor

import (
	"strings"
	"unicode"

	"github.com/lovoo/goka"
	"HW2/internal/gokahelper"
)

func isWordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsNumber(r)
}

func Filter(text string, isBlocked func(word string) bool) string {
	runes := []rune(text)
	var out strings.Builder

	for i := 0; i < len(runes); {
		if !isWordRune(runes[i]) {
			out.WriteRune(runes[i])
			i++
			continue
		}
		start := i
		for i < len(runes) && isWordRune(runes[i]) {
			i++
		}
		word := string(runes[start:i])
		if isBlocked(word) {
			out.WriteRune('*')
		} else {
			out.WriteString(word)
		}
	}
	return out.String()
}

func ViewHasWord(view *goka.View, word string) bool {
	if gokahelper.ViewHasKey(view, word) {
		return true
	}
	lower := strings.ToLower(word)
	return lower != word && gokahelper.ViewHasKey(view, lower)
}
