package channel

import (
	"strings"
	"testing"
)

func TestCorrelationDiagnosticsCompareSenderAcrossEventTypes(t *testing.T) {
	start := correlationShape([]byte(`{"stream_id":42,"from":"private-user","channel":"private-channel"}`))
	transcript := correlationShape([]byte(`{"sender":"private-user","channel":"private-channel","text":"private speech","metadata":{"streamId":42}}`))
	for _, value := range []string{`"private-user"`, `"private-channel"`, `42`} {
		token := diagnosticToken(value)
		if !strings.Contains(start, token) || !strings.Contains(transcript, token) {
			t.Fatalf("matching correlation metadata missing: %s / %s", start, transcript)
		}
	}
	if !strings.Contains(transcript, "metadata:{streamId:number@") {
		t.Fatalf("nested identifier not visible: %s", transcript)
	}
	for _, secret := range []string{"private-user", "private-channel", "private speech"} {
		if strings.Contains(start+transcript, secret) {
			t.Fatal("private content leaked")
		}
	}
}

func TestCorrelationDiagnosticsDoNotLogArbitraryWireContent(t *testing.T) {
	input := `{"sender":"SECRET","text":"SECRET","message":"SECRET","password":"SECRET","translations":[{"message":"SECRET"}],"SECRET\ninjection":{"id":"SECRET","other":"SECRET"},"metadata":{"token":"SECRET"}}`
	got := correlationShape([]byte(input))
	if strings.Contains(got, "SECRET") || strings.Contains(got, "injection") || strings.ContainsAny(got, "\r\n") {
		t.Fatalf("diagnostics exposed private wire content: %s", got)
	}
	if !strings.Contains(got, "text:<redacted>") || !strings.Contains(got, "translations:<redacted>") {
		t.Fatalf("speech was not redacted: %s", got)
	}
	if correlationShape([]byte(`{"sender":`)) != "unparsable" {
		t.Fatal("malformed event was not handled")
	}
}

func TestCorrelationDiagnosticsBoundNestedMetadata(t *testing.T) {
	got := correlationShape([]byte(`{"data":{"data":{"data":{"data":{"sender":"deep"}}}}}`))
	if !strings.Contains(got, "object(1 fields)") || strings.Contains(got, diagnosticToken(`"deep"`)) {
		t.Fatalf("depth bound not applied: %s", got)
	}
	if diagnosticToken(`"alice"`) == diagnosticToken(`"bob"`) {
		t.Fatal("different senders received the same token")
	}
}
