package data

import "testing"

func TestCacheTokenByTable(t *testing.T) {
	name := "test_conn"
	base := cacheToken(name, []string{"user"})
	cacheTouchTable(name, "order")
	afterOther := cacheToken(name, []string{"user"})
	if base != afterOther {
		t.Fatalf("user cache token should not change when order touched")
	}
	cacheTouchTable(name, "user")
	afterUser := cacheToken(name, []string{"user"})
	if base == afterUser {
		t.Fatalf("user cache token should change when user touched")
	}
}
