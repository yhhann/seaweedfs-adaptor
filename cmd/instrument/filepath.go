package instrument

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	PathLevel3 = iota + 2 // depth of directory is 3.
	PathLevel4            // depth of directory is 4.
	PathLevel5            // depth of directory is 5.
	PathLevel6            // depth of directory is 6.
)

// GetFilePath returns a file path according to its parameters.
func GetFilePath(baseDir string, domain int64, fn string, pathVer int, digit int) string {
	baseDir = strings.TrimSpace(baseDir)
	fn = strings.TrimSpace(fn)
	digit = sanitizeDigit(digit)

	switch pathVer {
	default:
		return getOldFilePath(baseDir, domain, fn)
	case PathLevel3:
		return getPathLevel3(baseDir, domain, fn, digit)
	case PathLevel4:
		return getPathLevel4(baseDir, domain, fn, digit)
	case PathLevel5:
		return getPathLevel5(baseDir, domain, fn, digit)
	case PathLevel6:
		return getPathLevel6(baseDir, domain, fn, digit)
	}
}

// Length of path truncated from fn must between 2 and 4.
// sanitizeDigit makes digit satisfing above rule.
func sanitizeDigit(digit int) int {
	switch {
	case digit < 2:
		return 2
	case digit > 4:
		return 4
	default:
		return digit
	}
}

func getZeros(digit int) string {
	switch digit {
	case 3:
		return "000"
	case 4:
		return "0000"
	default:
		return "00"
	}
}

func getPathLevel3(baseDir string, domain int64, fn string, digit int) string {
	result := filepath.Join(baseDir, fmt.Sprintf("g%d", domain%10000), fmt.Sprintf("%d", domain))

	if len(fn) > 0 {
		dummy := getZeros(digit)
		if len(fn) > digit {
			dummy = fn[0:digit]
		}
		result = filepath.Join(result, dummy, fn)
	}
	return result
}

func getPathLevel4(baseDir string, domain int64, fn string, digit int) string {
	result := filepath.Join(baseDir, fmt.Sprintf("g%d", domain%10000), fmt.Sprintf("%d", domain))

	if len(fn) > 0 {
		dummy := getZeros(digit)
		dummy2 := getZeros(digit)
		if len(fn) > digit {
			dummy = fn[0:digit]
		}
		if len(fn) > digit*2 {
			dummy2 = fn[digit : digit*2]
		}
		result = filepath.Join(result, dummy, dummy2, fn)
	}
	return result
}

func getPathLevel5(baseDir string, domain int64, fn string, digit int) string {
	result := filepath.Join(baseDir, fmt.Sprintf("g%d", domain%10000), fmt.Sprintf("%d", domain))

	if len(fn) > 0 {
		dummy := getZeros(digit)
		dummy2 := getZeros(digit)
		dummy3 := getZeros(digit)
		if len(fn) > digit {
			dummy = fn[0:digit]
		}
		if len(fn) > digit*2 {
			dummy2 = fn[digit : digit*2]
		}
		if len(fn) > digit*3 {
			dummy3 = fn[digit*2 : digit*3]
		}
		result = filepath.Join(result, dummy, dummy2, dummy3, fn)
	}
	return result
}

func getPathLevel6(baseDir string, domain int64, fn string, digit int) string {
	result := filepath.Join(baseDir, fmt.Sprintf("g%d", domain%10000), fmt.Sprintf("%d", domain))

	if len(fn) > 0 {
		dummy := getZeros(digit)
		dummy2 := getZeros(digit)
		dummy3 := getZeros(digit)
		dummy4 := getZeros(digit)
		if len(fn) > digit {
			dummy = fn[0:digit]
		}
		if len(fn) > digit*2 {
			dummy2 = fn[digit : digit*2]
		}
		if len(fn) > digit*3 {
			dummy3 = fn[digit*2 : digit*3]
		}
		if len(fn) > digit*4 {
			dummy4 = fn[digit*3 : digit*4]
		}
		result = filepath.Join(result, dummy, dummy2, dummy3, dummy4, fn)
	}
	return result
}

// getOldFilePath returns a path like "baseDir/domain/fn"
func getOldFilePath(baseDir string, domain int64, fn string) string {
	return filepath.Join(baseDir, fmt.Sprintf("%d", domain), fn)
}

func substr(s string, pos, length int) string {
	runes := []rune(s)
	l := pos + length
	if l > len(runes) {
		l = len(runes)
	}
	return string(runes[pos:l])
}

func GetParentDirectory(dirctory string) string {
	return substr(dirctory, 0, strings.LastIndex(dirctory, "/"))
}

func GetCurrentDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	return strings.Replace(dir, "\\", "/", -1)
}
