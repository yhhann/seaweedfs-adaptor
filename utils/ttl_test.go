package utils

import (
	"testing"
)

func Test(t *testing.T) {
	t.Logf("ttl 3m adjust to %s", AdjustTTL("3m"))
	t.Logf("ttl 50h adjust to %s", AdjustTTL("50h"))
	t.Logf("ttl 180d adjust to %s", AdjustTTL("180d"))
}
