package main

import (
	"bytes"
	"mime/multipart"
	"testing"
)

// buildCallBody builds an rdio-style multipart call-upload body with the given
// form fields (plus an audio part, which the parser must skip over).
func buildCallBody(t *testing.T, fields map[string]string) (contentType string, body []byte) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for k, v := range fields {
		if err := w.WriteField(k, v); err != nil {
			t.Fatalf("write field %s: %v", k, err)
		}
	}
	fw, err := w.CreateFormFile("audio", "call.mp3")
	if err != nil {
		t.Fatalf("create audio part: %v", err)
	}
	if _, err := fw.Write([]byte{0xff, 0xfb, 0x90, 0x00, 0x01, 0x02}); err != nil {
		t.Fatalf("write audio: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return w.FormDataContentType(), buf.Bytes()
}

// TestExtractCallMeta — the enrichment fields are parsed out of a stored
// multipart body, dateTime seconds are converted to millis, and the body bytes
// are untouched by the parse.
func TestExtractCallMeta(t *testing.T) {
	ct, body := buildCallBody(t, map[string]string{
		"key":       "abc",
		"system":    "2",
		"dateTime":  "1754600000",
		"talkgroup": "10101",
		"source":    "912345",
		"frequency": "142658000",
	})
	orig := append([]byte(nil), body...)

	meta, err := extractCallMeta(ct, body)
	if err != nil {
		t.Fatalf("extractCallMeta: %v", err)
	}
	if meta.talkgroup != 10101 || meta.source != 912345 || meta.frequency != 142658000 {
		t.Errorf("meta mismatch: %+v", meta)
	}
	if meta.tsMs != 1754600000000 {
		t.Errorf("dateTime seconds not converted to millis: %d", meta.tsMs)
	}
	if !bytes.Equal(body, orig) {
		t.Error("body bytes were modified by the parse")
	}
}

// TestExtractCallMetaMissingFields — absent numeric fields stay 0 and don't
// error (the caller skips enrichment when talkgroup<=0).
func TestExtractCallMetaMissingFields(t *testing.T) {
	ct, body := buildCallBody(t, map[string]string{"key": "abc"})
	meta, err := extractCallMeta(ct, body)
	if err != nil {
		t.Fatalf("extractCallMeta: %v", err)
	}
	if meta.talkgroup != 0 || meta.source != 0 || meta.frequency != 0 || meta.tsMs != 0 {
		t.Errorf("missing fields must stay 0: %+v", meta)
	}
}

// TestExtractCallMetaBadContentType — a non-multipart content type is an error
// (which the sender treats as "no enrichment", never an upload failure).
func TestExtractCallMetaBadContentType(t *testing.T) {
	if _, err := extractCallMeta("application/json", []byte("{}")); err == nil {
		t.Error("expected an error for a non-multipart content type")
	}
	if _, err := extractCallMeta("multipart/form-data", []byte("x")); err == nil {
		t.Error("expected an error for a multipart content type without boundary")
	}
}
