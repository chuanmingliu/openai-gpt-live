package reasoner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	MaxPrefixItems     = 32
	MaxPrefixBytes     = 64 << 10
	MaxPrefixLineBytes = 8 << 10
)

type PrefixLine struct {
	Role string `json:"role"`
	Text string `json:"text"`
}

type Prefix struct {
	Ref   string
	Hash  string
	Lines []PrefixLine
	Raw   json.RawMessage
}

func BuildPrefix(conversationID, ref string, lines []PrefixLine) (Prefix, error) {
	owned := make([]PrefixLine, 0, len(lines))
	total := 0
	for _, line := range lines {
		role := strings.TrimSpace(line.Role)
		text := strings.TrimSpace(line.Text)
		if role == "" || text == "" || !utf8.ValidString(role) || !utf8.ValidString(text) {
			continue
		}
		if role != RoleUser && role != RoleAssistant && role != RoleSystem {
			continue
		}
		if len(text) > MaxPrefixLineBytes {
			text = string([]rune(text)[:maxRunes(text, MaxPrefixLineBytes)])
		}
		if total+len(role)+len(text) > MaxPrefixBytes || len(owned) >= MaxPrefixItems {
			break
		}
		owned = append(owned, PrefixLine{Role: role, Text: text})
		total += len(role) + len(text)
	}
	if conversationID == "" {
		conversationID = "conversation"
	}
	if strings.TrimSpace(ref) == "" {
		ref = "conversation:" + conversationID
	}
	raw, err := json.Marshal(owned)
	if err != nil {
		return Prefix{}, err
	}
	digest := sha256.Sum256(raw)
	return Prefix{Ref: ref, Hash: hex.EncodeToString(digest[:]), Lines: owned, Raw: raw}, nil
}

func ParsePrefix(raw json.RawMessage) ([]PrefixLine, error) {
	lines, _, err := parsePrefix(raw)
	return lines, err
}

func VerifyPrefix(raw json.RawMessage, hash string) ([]PrefixLine, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	lines, built, err := parsePrefix(raw)
	if err != nil {
		return nil, err
	}
	if hash != "" && built.Hash != hash {
		return nil, errors.New("context prefix hash mismatch")
	}
	return lines, nil
}

func parsePrefix(raw json.RawMessage) ([]PrefixLine, Prefix, error) {
	if len(raw) == 0 {
		return nil, Prefix{}, nil
	}
	var lines []PrefixLine
	if err := json.Unmarshal(raw, &lines); err != nil {
		return nil, Prefix{}, err
	}
	built, err := BuildPrefix("", "", lines)
	if err != nil {
		return nil, Prefix{}, err
	}
	return built.Lines, built, nil
}

func IsBackchannel(text string) bool {
	normalized := strings.ToLower(strings.TrimSpace(text))
	normalized = strings.Trim(normalized, ".!,?…")
	normalized = strings.Join(strings.Fields(normalized), " ")
	switch normalized {
	case "mm", "mmm", "mhm", "mmhmm", "mm-hmm", "mm hmm", "hmm", "huh",
		"uh-huh", "uh huh", "yeah", "yep", "yup", "ok", "okay", "right",
		"sure", "got it", "gotcha":
		return true
	default:
		return false
	}
}

func maxRunes(value string, byteLimit int) int {
	if len(value) <= byteLimit {
		return len([]rune(value))
	}
	count := 0
	used := 0
	for _, r := range value {
		size := utf8.RuneLen(r)
		if size < 0 || used+size > byteLimit {
			break
		}
		used += size
		count++
	}
	return count
}
