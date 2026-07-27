package pagerdecode

import "testing"

func TestParseLine(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		wantOK  bool
		wantMsg Message
	}{
		{
			name:   "alpha 1200",
			line:   "POCSAG1200: Address:  1234567  Function: 0  Alpha:   SOME MESSAGE TEXT",
			wantOK: true,
			wantMsg: Message{
				Address:  "1234567",
				Function: 0,
				Alpha:    true,
				Text:     "SOME MESSAGE TEXT",
			},
		},
		{
			name:   "numeric 512",
			line:   "POCSAG512: Address:  0987654  Function: 3  Numeric:  123 456",
			wantOK: true,
			wantMsg: Message{
				Address:  "0987654",
				Function: 3,
				Alpha:    false,
				Text:     "123 456",
			},
		},
		{
			name:   "alpha 2400",
			line:   "POCSAG2400: Address: 42  Function: 2  Alpha: HELLO WORLD",
			wantOK: true,
			wantMsg: Message{
				Address:  "42",
				Function: 2,
				Alpha:    true,
				Text:     "HELLO WORLD",
			},
		},
		{
			name:   "alpha empty payload",
			line:   "POCSAG1200: Address: 1000000  Function: 1  Alpha:",
			wantOK: true,
			wantMsg: Message{
				Address:  "1000000",
				Function: 1,
				Alpha:    true,
				Text:     "",
			},
		},
		{
			name:   "trailing CRLF trimmed",
			line:   "POCSAG1200: Address: 5551234  Function: 0  Alpha: PADDED  \r\n",
			wantOK: true,
			wantMsg: Message{
				Address:  "5551234",
				Function: 0,
				Alpha:    true,
				Text:     "PADDED",
			},
		},
		{
			name:   "trailing NUL padding stripped",
			line:   "POCSAG1200: Address: 180111  Function: 0  Alpha: 1421\x00\x00",
			wantOK: true,
			wantMsg: Message{
				Address:  "180111",
				Function: 0,
				Alpha:    true,
				Text:     "1421",
			},
		},
		{
			name:   "interior + trailing control bytes cleaned",
			line:   "POCSAG1200: Address: 146126  Function: 0  Alpha: FRINC TYPE: AFA TURNOUT: 376 INC: 146126-27072026\x00 \x00",
			wantOK: true,
			wantMsg: Message{
				Address:  "146126",
				Function: 0,
				Alpha:    true,
				Text:     "FRINC TYPE: AFA TURNOUT: 376 INC: 146126-27072026",
			},
		},
		{
			name:   "trailing EOT mnemonics (literal text) stripped",
			line:   "POCSAG1200: Address: 420008  Function: 3  Alpha: FIRECALL OLD GLEN INNES RD [152.66218,-29.848288] EOT EOT",
			wantOK: true,
			wantMsg: Message{
				Address:  "420008",
				Function: 3,
				Alpha:    true,
				Text:     "FIRECALL OLD GLEN INNES RD [152.66218,-29.848288]",
			},
		},
		{
			name:   "leading STX + trailing EOT mnemonic stripped",
			line:   "POCSAG1200: Address: 420008  Function: 3  Alpha: STX HELLO WORLD EOT",
			wantOK: true,
			wantMsg: Message{
				Address:  "420008",
				Function: 3,
				Alpha:    true,
				Text:     "HELLO WORLD",
			},
		},
		{
			name:   "raw EOT control byte stripped",
			line:   "POCSAG1200: Address: 420008  Function: 3  Alpha: HELLO\x04\x04",
			wantOK: true,
			wantMsg: Message{
				Address:  "420008",
				Function: 3,
				Alpha:    true,
				Text:     "HELLO",
			},
		},
		{
			name:   "real word CAN not stripped",
			line:   "POCSAG1200: Address: 420008  Function: 3  Alpha: DO WHAT YOU CAN",
			wantOK: true,
			wantMsg: Message{
				Address:  "420008",
				Function: 3,
				Alpha:    true,
				Text:     "DO WHAT YOU CAN",
			},
		},
		{
			name:   "non-matching banner",
			line:   "Enabled demodulators: POCSAG512 POCSAG1200 POCSAG2400",
			wantOK: false,
		},
		{
			name:   "empty line",
			line:   "",
			wantOK: false,
		},
		{
			name:   "unrelated protocol",
			line:   "FLEX: 2009-01-01 00:00:00 1600/2/K/A 12.345 [001234567] ALN hello",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, ok := ParseLine(tt.line)
			if ok != tt.wantOK {
				t.Fatalf("ParseLine(%q) ok = %v, want %v", tt.line, ok, tt.wantOK)
			}
			if !tt.wantOK {
				if msg != nil {
					t.Fatalf("ParseLine(%q) returned non-nil msg on failure: %+v", tt.line, msg)
				}
				return
			}
			if msg == nil {
				t.Fatalf("ParseLine(%q) returned nil msg on success", tt.line)
			}
			if *msg != tt.wantMsg {
				t.Fatalf("ParseLine(%q) = %+v, want %+v", tt.line, *msg, tt.wantMsg)
			}
		})
	}
}
