package service_test

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"qrok/internal/controlplane/infrastructure/auth"
	"qrok/internal/controlplane/infrastructure/delivery"
	"qrok/internal/controlplane/infrastructure/eventstore"
	"qrok/internal/controlplane/model"
)

type tokenRec struct {
	tokenID   string
	projectID string
	userID    string
}

type deviceRec struct {
	userCode    string
	status      model.DeviceStatus
	expiresAt   time.Time
	accessToken *string
	projectID   string
	devTokenID  string
}

type fakeAuthRepo struct {
	mu sync.Mutex

	apiTokens     map[string]tokenRec
	agentTokens   map[string]tokenRec // key: hash+"|"+tunnelID
	devTokens     map[string]tokenRec // key: hash+"|"+tunnelID
	tunnelProject map[string]string
	eventProject  map[string]string
	projects      map[string]bool
	devices       map[string]*deviceRec
	userCodeHash  map[string]string

	insertDeviceErr error
	withinTxErr     error
}

func newFakeAuthRepo() *fakeAuthRepo {
	return &fakeAuthRepo{
		apiTokens:     make(map[string]tokenRec),
		agentTokens:   make(map[string]tokenRec),
		devTokens:     make(map[string]tokenRec),
		tunnelProject: make(map[string]string),
		eventProject:  make(map[string]string),
		projects:      make(map[string]bool),
		devices:       make(map[string]*deviceRec),
		userCodeHash:  make(map[string]string),
	}
}

func (f *fakeAuthRepo) addAPIToken(plaintext, tokenID, projectID string) {
	f.apiTokens[auth.HashAgentToken(plaintext)] = tokenRec{tokenID: tokenID, projectID: projectID}
}

func (f *fakeAuthRepo) addAgentToken(plaintext, tunnelID, tokenID, projectID string) {
	key := auth.HashAgentToken(plaintext) + "|" + tunnelID
	f.agentTokens[key] = tokenRec{tokenID: tokenID, projectID: projectID}
	f.tunnelProject[tunnelID] = projectID
}

func (f *fakeAuthRepo) addDevToken(plaintext, tunnelID, tokenID, projectID, userID string) {
	key := auth.HashAgentToken(plaintext) + "|" + tunnelID
	f.devTokens[key] = tokenRec{tokenID: tokenID, projectID: projectID, userID: userID}
	f.tunnelProject[tunnelID] = projectID
}

func (f *fakeAuthRepo) addDevice(hash string, rec deviceRec) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.devices[hash] = &rec
	f.userCodeHash[rec.userCode] = hash
}

func (f *fakeAuthRepo) FindAgentToken(_ context.Context, tokenHash, tunnelID string) (string, string, error) {
	rec, ok := f.agentTokens[tokenHash+"|"+tunnelID]
	if !ok {
		return "", "", pgx.ErrNoRows
	}
	return rec.tokenID, rec.projectID, nil
}

func (f *fakeAuthRepo) FindAPIToken(_ context.Context, tokenHash string) (string, string, error) {
	rec, ok := f.apiTokens[tokenHash]
	if !ok {
		return "", "", pgx.ErrNoRows
	}
	return rec.tokenID, rec.projectID, nil
}

func (f *fakeAuthRepo) FindDevToken(_ context.Context, tokenHash, tunnelID string) (string, string, string, error) {
	rec, ok := f.devTokens[tokenHash+"|"+tunnelID]
	if !ok {
		return "", "", "", pgx.ErrNoRows
	}
	return rec.tokenID, rec.projectID, rec.userID, nil
}

func (f *fakeAuthRepo) TunnelOwnedByProject(_ context.Context, projectID, tunnelID string) (bool, error) {
	return f.tunnelProject[tunnelID] == projectID, nil
}

func (f *fakeAuthRepo) EventOwnedByProject(_ context.Context, projectID, eventID string) (bool, error) {
	return f.eventProject[eventID] == projectID, nil
}

func (f *fakeAuthRepo) ProjectExists(_ context.Context, projectID string) (bool, error) {
	return f.projects[projectID], nil
}

func (f *fakeAuthRepo) InsertDeviceAuthorization(_ context.Context, deviceHash, userCode, projectID string, pollInterval int, expiresAt time.Time) error {
	if f.insertDeviceErr != nil {
		return f.insertDeviceErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.devices[deviceHash] = &deviceRec{
		userCode:  userCode,
		status:    model.DeviceStatusPending,
		expiresAt: expiresAt,
		projectID: projectID,
	}
	f.userCodeHash[userCode] = deviceHash
	return nil
}

func (f *fakeAuthRepo) GetDeviceByHash(_ context.Context, deviceHash string) (model.DeviceStatus, time.Time, *string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.devices[deviceHash]
	if !ok {
		return "", time.Time{}, nil, pgx.ErrNoRows
	}
	return rec.status, rec.expiresAt, rec.accessToken, nil
}

func (f *fakeAuthRepo) UpdateDeviceStatus(_ context.Context, deviceHash string, status model.DeviceStatus) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec, ok := f.devices[deviceHash]; ok {
		rec.status = status
	}
	return nil
}

func (f *fakeAuthRepo) ConsumeDeviceToken(_ context.Context, deviceHash string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec, ok := f.devices[deviceHash]; ok {
		rec.status = model.DeviceStatusConsumed
		rec.accessToken = nil
	}
	return nil
}

func (f *fakeAuthRepo) WithinTx(ctx context.Context, fn func(auth.TxRepository) error) error {
	if f.withinTxErr != nil {
		return f.withinTxErr
	}
	return fn(&fakeAuthTx{parent: f})
}

func (f *fakeAuthRepo) InsertDevToken(context.Context, string, string, string, string) error {
	return nil
}

type fakeAuthTx struct {
	parent *fakeAuthRepo
}

func (tx *fakeAuthTx) GetDeviceByUserCodeForUpdate(_ context.Context, userCode string) (string, model.DeviceStatus, time.Time, error) {
	tx.parent.mu.Lock()
	defer tx.parent.mu.Unlock()
	hash, ok := tx.parent.userCodeHash[userCode]
	if !ok {
		return "", "", time.Time{}, pgx.ErrNoRows
	}
	rec := tx.parent.devices[hash]
	return hash, rec.status, rec.expiresAt, nil
}

func (tx *fakeAuthTx) UpdateDeviceStatus(ctx context.Context, deviceHash string, status model.DeviceStatus) error {
	return tx.parent.UpdateDeviceStatus(ctx, deviceHash, status)
}

func (tx *fakeAuthTx) ApproveDevice(_ context.Context, deviceHash, projectID, devTokenID, accessToken string) error {
	tx.parent.mu.Lock()
	defer tx.parent.mu.Unlock()
	rec := tx.parent.devices[deviceHash]
	rec.status = model.DeviceStatusApproved
	rec.projectID = projectID
	rec.devTokenID = devTokenID
	rec.accessToken = &accessToken
	return nil
}

func (tx *fakeAuthTx) InsertDevToken(context.Context, string, string, string, string) error {
	return nil
}

func (tx *fakeAuthTx) ProjectExists(_ context.Context, projectID string) (bool, error) {
	return tx.parent.projects[projectID], nil
}

type fakeEventRepo struct {
	events    map[string]*model.Event
	insertErr error
}

func newFakeEventRepo() *fakeEventRepo {
	return &fakeEventRepo{events: make(map[string]*model.Event)}
}

func (f *fakeEventRepo) Insert(_ context.Context, ev *model.Event) (bool, error) {
	if f.insertErr != nil {
		return false, f.insertErr
	}
	if _, exists := f.events[ev.ID]; exists {
		return false, nil
	}
	cp := *ev
	f.events[ev.ID] = &cp
	return true, nil
}

func (f *fakeEventRepo) GetByID(_ context.Context, id string) (*model.Event, error) {
	ev, ok := f.events[id]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	cp := *ev
	return &cp, nil
}

func (f *fakeEventRepo) GetByIDForProject(_ context.Context, projectID, eventID string) (*model.Event, error) {
	ev, ok := f.events[eventID]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	// Caller is responsible for project scoping via auth; fake stores events only.
	_ = projectID
	cp := *ev
	return &cp, nil
}

func (f *fakeEventRepo) GetPayload(_ context.Context, ev *model.Event) ([]byte, error) {
	if len(ev.Payload) > 0 {
		return ev.Payload, nil
	}
	return nil, pgx.ErrNoRows
}

func (f *fakeEventRepo) ListByTunnel(_ context.Context, tunnelID string, limit int) ([]*model.Event, error) {
	var out []*model.Event
	for _, ev := range f.events {
		if ev.TunnelID == tunnelID {
			cp := *ev
			out = append(out, &cp)
		}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type fakeDeliveryRepo struct {
	mu sync.Mutex

	pending   []*model.Delivery
	upserted  []*model.Delivery
	createErr error
	upsertErr error
}

func (f *fakeDeliveryRepo) CreatePending(_ context.Context, rec *model.Delivery) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *rec
	f.pending = append(f.pending, &cp)
	return nil
}

func (f *fakeDeliveryRepo) UpsertResult(_ context.Context, rec *model.Delivery) error {
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *rec
	f.upserted = append(f.upserted, &cp)
	return nil
}

func (f *fakeDeliveryRepo) GetByID(_ context.Context, id string) (*model.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, rec := range f.upserted {
		if rec.ID == id {
			cp := *rec
			return &cp, nil
		}
	}
	for _, rec := range f.pending {
		if rec.ID == id {
			cp := *rec
			return &cp, nil
		}
	}
	return nil, pgx.ErrNoRows
}

func (f *fakeDeliveryRepo) ListByEventID(_ context.Context, eventID string) ([]*model.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*model.Delivery
	for _, rec := range append(f.pending, f.upserted...) {
		if rec.EventID == eventID {
			cp := *rec
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (f *fakeDeliveryRepo) ListLatestByEventIDs(_ context.Context, eventIDs []string) (map[string]*model.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	want := make(map[string]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		want[id] = struct{}{}
	}
	out := make(map[string]*model.Delivery)
	for _, rec := range append(f.pending, f.upserted...) {
		if _, ok := want[rec.EventID]; !ok {
			continue
		}
		if existing, ok := out[rec.EventID]; !ok || rec.CreatedAt.After(existing.CreatedAt) {
			cp := *rec
			out[rec.EventID] = &cp
		}
	}
	return out, nil
}

var (
	_ auth.Repository       = (*fakeAuthRepo)(nil)
	_ auth.TxRepository     = (*fakeAuthTx)(nil)
	_ eventstore.Repository = (*fakeEventRepo)(nil)
	_ delivery.Repository   = (*fakeDeliveryRepo)(nil)
)
