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
			name:   "trailing bracketed EOT mnemonic (multimon form) stripped",
			line:   "POCSAG1200: Address: 870016  Function: 3  Alpha: CVDO - 26-121978 - [152.942958,-29.706606] <EOT>",
			wantOK: true,
			wantMsg: Message{
				Address:  "870016",
				Function: 3,
				Alpha:    true,
				Text:     "CVDO - 26-121978 - [152.942958,-29.706606]",
			},
		},
		{
			name:   "fused double bracketed EOT stripped",
			line:   "POCSAG1200: Address: 420008  Function: 3  Alpha: FIRECALL [152.66218,-29.848288]<EOT><EOT>",
			wantOK: true,
			wantMsg: Message{
				Address:  "420008",
				Function: 3,
				Alpha:    true,
				Text:     "FIRECALL [152.66218,-29.848288]",
			},
		},
		{
			name:   "leading STX + trailing ETX bracketed mnemonics stripped",
			line:   "POCSAG1200: Address: 420008  Function: 3  Alpha: <STX>HELLO WORLD<ETX>",
			wantOK: true,
			wantMsg: Message{
				Address:  "420008",
				Function: 3,
				Alpha:    true,
				Text:     "HELLO WORLD",
			},
		},
		{
			name:   "national chars mapped to brackets",
			line:   "POCSAG1200: Address: 420008  Function: 3  Alpha: ÄTEST MESSAGEÜ",
			wantOK: true,
			wantMsg: Message{
				Address:  "420008",
				Function: 3,
				Alpha:    true,
				Text:     "[TEST MESSAGE]",
			},
		},
		{
			name:   "raw NUL padding bytes stripped",
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
			name:   "real unbracketed words (incl. CAN) not stripped",
			line:   "POCSAG1200: Address: 420008  Function: 3  Alpha: DO WHAT YOU CAN EOT ROAD",
			wantOK: true,
			wantMsg: Message{
				Address:  "420008",
				Function: 3,
				Alpha:    true,
				Text:     "DO WHAT YOU CAN EOT ROAD",
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
