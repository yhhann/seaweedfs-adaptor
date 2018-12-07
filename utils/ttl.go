package utils

import (
	"fmt"
	"strconv"
	"strings"
)

func AdjustTTL(ttlString string) string {
	if ttlString == "" {
		return ""
	}

	ttlBytes := []byte(ttlString)
	unitByte := ttlBytes[len(ttlBytes)-1]
	countBytes := ttlBytes[0 : len(ttlBytes)-1]
	if '0' <= unitByte && unitByte <= '9' {
		countBytes = ttlBytes
		unitByte = 'm'
	}
	count, _ := strconv.Atoi(string(countBytes))
	count++

	return fmt.Sprintf("%d%c", count, unitByte)
}

func SanitizeTTL(url string, ttl string) (ret string) {
	if ttl == "" {
		return url
	}

	if strings.Index(url, "ttl") >= 0 {
		return url
	}

	if strings.Index(url, "?") >= 0 {
		return url + "&ttl=" + ttl
	}

	return url + "?ttl=" + ttl
}
