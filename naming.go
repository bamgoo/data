package data

import (
	"strings"
	"unicode"
)

func SnakeFieldPath(field string) string {
	field = strings.TrimSpace(field)
	if field == "" {
		return field
	}
	parts := strings.Split(field, ".")
	if len(parts) == 1 {
		return snakeWord(parts[0])
	}
	parts[len(parts)-1] = snakeWord(parts[len(parts)-1])
	return strings.Join(parts, ".")
}

func CamelFieldPath(field string) string {
	field = strings.TrimSpace(field)
	if field == "" {
		return field
	}
	parts := strings.Split(field, ".")
	if len(parts) == 1 {
		return camelWord(parts[0])
	}
	parts[len(parts)-1] = camelWord(parts[len(parts)-1])
	return strings.Join(parts, ".")
}

func snakeWord(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	out := make([]rune, 0, len(s)+4)
	var prev rune
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 && prev != '_' && (unicode.IsLower(prev) || unicode.IsDigit(prev)) {
				out = append(out, '_')
			}
			out = append(out, unicode.ToLower(r))
		} else {
			out = append(out, r)
		}
		prev = r
	}
	return string(out)
}

func camelWord(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || !strings.Contains(s, "_") {
		return s
	}
	parts := strings.Split(s, "_")
	out := parts[0]
	for i := 1; i < len(parts); i++ {
		p := parts[i]
		if p == "" {
			continue
		}
		rs := []rune(p)
		rs[0] = unicode.ToUpper(rs[0])
		out += string(rs)
	}
	return out
}
