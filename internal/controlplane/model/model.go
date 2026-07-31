// Package model contains transport- and storage-independent control-plane data.
package model

import "time"

// Subject identifies an authenticated caller and its access scope.
type Subject struct {
	TokenID   string
	ProjectID string
	UserID    string
	TunnelID  string
	AllowAll  bool
}

// Event is an event stored and delivered by the control plane.
type Event struct {
	ID           string
	TunnelID     string
	SourceType   string
	Topic        string
	Partition    *int32
	BrokerOffset *int64
	Key          []byte
	Headers      map[string]string
	Payload      []byte
	PayloadRef   string
	PayloadSize  int32
	IsReplay     bool
	BrokerTS     *time.Time
	CreatedAt    time.Time
}

// DeliveryKind identifies how an event delivery was initiated.
type DeliveryKind string

const (
	DeliveryKindLive   DeliveryKind = "live"
	DeliveryKindReplay DeliveryKind = "replay"
)

// DeliveryStatus is the persisted state of a delivery attempt.
type DeliveryStatus string

const (
	DeliveryStatusPending   DeliveryStatus = "pending"
	DeliveryStatusDelivered DeliveryStatus = "delivered"
	DeliveryStatusFailed    DeliveryStatus = "failed"
)

// Delivery is one attempt to deliver an event to a development client.
type Delivery struct {
	ID         string
	EventID    string
	TargetID   string
	Kind       DeliveryKind
	Status     DeliveryStatus
	StatusCode *int32
	Error      string
	LatencyMS  *int32
	CreatedAt  time.Time
}

// DeliveryResult is a transport-independent delivery acknowledgement.
type DeliveryResult struct {
	DeliveryID string
	EventID    string
	StatusCode int32
	Error      string
	LatencyMS  int32
}

// DeviceStatus is the state of an OAuth device authorization.
type DeviceStatus string

const (
	DeviceStatusPending  DeviceStatus = "pending"
	DeviceStatusApproved DeviceStatus = "approved"
	DeviceStatusDenied   DeviceStatus = "denied"
	DeviceStatusExpired  DeviceStatus = "expired"
	DeviceStatusConsumed DeviceStatus = "consumed"
)

// DeviceSession is the persisted state of an OAuth device authorization.
type DeviceSession struct {
	DeviceCodeHash string
	UserCode       string
	Status         DeviceStatus
	ProjectID      string
	DevTokenID     string
	AccessToken    string
	PollInterval   int
	ExpiresAt      time.Time
}

// DeviceStart is returned when a device authorization starts.
type DeviceStart struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

// DeviceTokenPoll is returned while polling a device authorization.
type DeviceTokenPoll struct {
	AccessToken string `json:"access_token,omitempty"`
	TokenType   string `json:"token_type,omitempty"`
	Error       string `json:"error,omitempty"`
	Description string `json:"error_description,omitempty"`
}
