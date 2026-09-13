package channel

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Diagnostic tokens are comparable within this process only. The random key is
// never persisted: logs contain neither usernames nor reusable identity hashes.
var diagnosticKey = func() []byte {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic("cannot initialize private diagnostic tokens")
	}
	return key
}()

func diagnosticToken(value string) string {
	h := hmac.New(sha256.New, diagnosticKey)
	h.Write([]byte(value))
	return fmt.Sprintf("%x", h.Sum(nil)[:8])
}

// correlationShape inspects the full event structure without copying speech or
// arbitrary wire strings into the log. Known metadata gets opaque equality
// tokens; unknown field names get tokens too, since names can contain content.
// Bounds keep malformed/nested events from producing unbounded diagnostics.
func correlationShape(data []byte) string {
	var raw any
	if json.Unmarshal(data, &raw) != nil {
		return "unparsable"
	}
	return diagnosticShape(raw, "", 0)
}

func diagnosticShape(value any, field string, depth int) string {
	switch field {
	case "text", "message", "translations", "password", "token", "auth_token", "refresh_token":
		return "<redacted>"
	}
	switch v := value.(type) {
	case map[string]any:
		if depth >= 3 {
			return fmt.Sprintf("object(%d fields)", len(v))
		}
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, 25)
		for i, key := range keys {
			if i == 24 {
				parts = append(parts, "...")
				break
			}
			label := key
			switch key {
			case "command", "stream_id", "streamId", "message_id", "messageId", "id", "sender", "from", "channel", "language", "confidence", "truncated", "text", "message", "translations", "stream", "metadata", "data", "type":
			default:
				label = "field@" + diagnosticToken(key)
			}
			parts = append(parts, label+":"+diagnosticShape(v[key], key, depth+1))
		}
		return "{" + strings.Join(parts, ",") + "}"
	case []any:
		// Array entries can contain arbitrary speech. Count them, don't copy.
		return fmt.Sprintf("array(%d)", len(v))
	case string, float64:
		kind := "string"
		if _, ok := v.(float64); ok {
			kind = "number"
		}
		switch field {
		case "stream_id", "streamId", "message_id", "messageId", "id", "sender", "from", "channel", "language":
			encoded, _ := json.Marshal(v)
			return kind + "@" + diagnosticToken(string(encoded))
		}
		return kind
	case bool:
		return "boolean"
	case nil:
		return "null"
	default:
		return "unknown"
	}
}
