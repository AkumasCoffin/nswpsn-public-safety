// Package enrol trades a single-use enrolment code for this node's token.
//
// A fresh install has no credential: the installer writes an enrolment code and
// an empty node_token. On first run the agent presents the code together with
// the machine id it will use, receives a token, and writes that token back into
// its config — spending the code in the process.
//
// This is why re-downloading an installer no longer disturbs a running node.
// The token used to be baked into the script, and because it is hashed at rest
// and never re-derivable, the download had to mint a new one — killing whatever
// agent was already running. Now the credential changes when an installer is
// RUN, which is the moment the operator actually meant.
package enrol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Timeout for the exchange. Generous: this happens once, at install, and a slow
// first response is far better than an install that needs running twice.
const timeout = 30 * time.Second

type request struct {
	Code      string `json:"code"`
	InstallID string `json:"installId"`
	Kind      string `json:"kind,omitempty"`
}

type response struct {
	Token  string `json:"token"`
	NodeID string `json:"nodeId"`
	Kind   string `json:"kind"`
	Error  string `json:"error"`
}

// ErrPermanent marks a refusal that retrying cannot fix — an unknown or expired
// code. The caller surfaces it and stops, rather than looping forever against a
// credential that will never work; the operator needs to download the installer
// again, and only they can do that.
type ErrPermanent struct{ Msg string }

func (e *ErrPermanent) Error() string { return e.Msg }

// Exchange posts the code and returns the node token.
func Exchange(serverURL, code, installID, kind, userAgent string) (string, error) {
	body, err := json.Marshal(request{Code: code, InstallID: installID, Kind: kind})
	if err != nil {
		return "", err
	}
	url := strings.TrimSuffix(serverURL, "/") + "/api/node-enrol"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)

	res, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return "", fmt.Errorf("enrol request failed: %w", err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))

	var parsed response
	_ = json.Unmarshal(raw, &parsed)

	switch {
	case res.StatusCode == http.StatusOK && parsed.Token != "":
		return parsed.Token, nil
	case res.StatusCode == http.StatusUnauthorized ||
		res.StatusCode == http.StatusBadRequest ||
		res.StatusCode == http.StatusConflict:
		// The code is unknown or spent (401/400), or this machine is already
		// enrolled under another account (409). None of those change by asking
		// again, and retrying a 409 every twenty seconds forever just buries
		// the one message that says what to do about it.
		msg := parsed.Error
		if msg == "" {
			msg = "enrolment refused"
		}
		return "", &ErrPermanent{Msg: msg}
	default:
		// 429, 503, 5xx, or a malformed 200: transient, so the caller retries.
		return "", fmt.Errorf("enrol failed: status %d: %s", res.StatusCode, strings.TrimSpace(string(raw)))
	}
}
