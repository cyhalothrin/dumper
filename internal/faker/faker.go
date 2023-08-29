package faker

import (
	"fmt"
	"math/rand"

	"github.com/lucasjones/reggen"

	"github.com/cyhalothrin/dumper/internal/config"
)

func Format(conf *config.FakerConfig) string {
	switch {
	case conf.Type == "phone":
		return generatePhone(conf.Pattern)
	case conf.Type == "email":
		return generateEmail()
	case conf.Type == "name":
		if conf.NamePart == "first" {
			return generateFirstName()
		} else if conf.NamePart == "last" {
			return generateLastName()
		}

		return generateFullName()
	case conf.Type == "hash":
		length := conf.Length
		if length == 0 {
			length = 32
		}

		return generateRandomString(length)
	default:
		panic(fmt.Sprintf("unsupported faker type %s", conf.Type))
	}
}

func generatePhone(pattern string) string {
	var (
		regexPattern []rune
		digits       int
	)

	for _, r := range pattern {
		if r == '*' {
			digits++
		} else {
			if digits > 0 {
				regexPattern = append(regexPattern, []rune(fmt.Sprintf("\\d{%d}", digits))...)
				digits = 0
			}

			regexPattern = append(regexPattern, r)
		}
	}

	if digits > 0 {
		regexPattern = append(regexPattern, []rune(fmt.Sprintf("\\d{%d}", digits))...)
	}

	str, err := reggen.Generate(string(regexPattern), 1)
	if err != nil {
		panic(fmt.Sprintf("failed to generate phone for patter %s: %s", pattern, err))
	}

	return str
}

func generateEmail() string {
	topLevelDomains := []string{"com", "net", "org", "gov"}

	return generateFirstName() + "." +
		generateLastName() + "@" +
		generateRandomString(5) + "." +
		topLevelDomains[rand.Intn(len(topLevelDomains))]
}

func generateRandomString(n int) string {
	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	s := make([]rune, n)
	for i := range s {
		s[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(s)
}
