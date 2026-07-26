// Package protocol defines the JSON WebSocket wire protocol spoken between the
// node agent and the backend. All frames are JSON text frames wrapping an
// Envelope: { "t": type, "id"?: correlation, "data"?: payload }.
package protocol

import "encoding/json"

// ProtocolVersion is the protocol revision this agent implements.
const ProtocolVersion = 1

// Outgoing message types (agent -> server).
const (
	TypeHello         = "hello"
	TypeStatus        = "status"
	TypeEvent         = "event"
	TypeConfigApplied = "configApplied"
	TypeConfigError   = "configError"
	TypeCmdResult     = "cmdResult"
)

// Incoming message types (server -> agent).
const (
	TypeHelloAck      = "helloAck"
	TypeConfigPush    = "configPush"
	TypeCmd           = "cmd"
	TypeSpectrumStart = "spectrumStart"
	TypeSpectrumStop  = "spectrumStop"
	TypeDisabled      = "disabled"
)

// Envelope is the outer frame for every message.
type Envelope struct {
	T    string          `json:"t"`
	ID   string          `json:"id,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// Hello is sent immediately after the WS connects to identify the agent.
type Hello struct {
	ProtocolVersion      int    `json:"protocolVersion"`
	AgentVersion         string `json:"agentVersion"`
	SDRTrunkVersion      string `json:"sdrtrunkVersion"`
	RdioVersion          string `json:"rdioVersion"`
	OS                   string `json:"os"`
	Arch                 string `json:"arch"`
	Hostname             string `json:"hostname"`
	AppliedConfigVersion string `json:"appliedConfigVersion"`
	// Kind is the node type this agent runs as (radio/pager/adsb).
	Kind string `json:"kind"`
}

// Status is the periodic heartbeat describing the agent's live state.
type Status struct {
	Tuners        []any             `json:"tuners"`
	Channels      []any             `json:"channels"`
	ActiveCalls   []any             `json:"activeCalls"`
	Events        []any             `json:"events"`
	Components    map[string]string `json:"components"`
	QueueDepth    int               `json:"queueDepth"`
	CPUPct        float64           `json:"cpuPct"`
	MemMB         int               `json:"memMB"`
	DiskFreeMB    int               `json:"diskFreeMB"`
	ConfigVersion *string           `json:"configVersion"`
	// Node readiness reported by the SDR-Trunk control server (null on older
	// builds): CPU calibration done, and the JMBE voice codec installed.
	Calibrated    *bool `json:"calibrated"`
	JmbeInstalled *bool `json:"jmbeInstalled"`
}

// HelloAck is the server's response to Hello.
type HelloAck struct {
	OK                    bool    `json:"ok"`
	ServerProtocolVersion int     `json:"serverProtocolVersion"`
	ConfigVersion         *string `json:"configVersion"`
	UpdateAvailable       bool    `json:"updateAvailable"`
}

// Command is the payload of an incoming "cmd" frame.
type Command struct {
	Action string          `json:"action"`
	Args   json.RawMessage `json:"args,omitempty"`
}

// CmdResult is the agent's reply to a command, echoing the incoming Envelope id.
type CmdResult struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}

// ConfigApplied acks a successful configPush apply.
type ConfigApplied struct {
	ConfigVersion string `json:"configVersion"`
}

// ConfigError reports a failed configPush apply and the stage that failed.
type ConfigError struct {
	Stage   string `json:"stage"`
	Message string `json:"message"`
}

// Marshal builds an Envelope with type t, optional correlation id, and a JSON
// payload built from data (which may be nil for an empty data field).
func Marshal(t string, data any, id string) ([]byte, error) {
	env := Envelope{T: t, ID: id}
	if data != nil {
		raw, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		env.Data = raw
	}
	return json.Marshal(env)
}

// ParseEnvelope decodes a raw frame into an Envelope.
func ParseEnvelope(b []byte) (*Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, err
	}
	return &env, nil
}
