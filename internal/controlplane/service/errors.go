// Package service contains control-plane application use cases.
package service

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"qrok/internal/controlplane/infrastructure/eventstore"
	"qrok/pkg/fault"
)

func internalErr(op string, err error) error {
	if err == nil {
		return nil
	}
	return fault.ErrInternal.Wrap(err, "internal error").WithOp(op)
}

func unavailableErr(op string, err error) error {
	if err == nil {
		return nil
	}
	return fault.ErrServiceUnavail.Wrap(err, "service temporarily unavailable").WithOp(op)
}

func notFoundErr(op, message string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, eventstore.ErrCorruptEvent) {
		return fault.ErrNotFound.New(message).WithOp(op)
	}
	return mapRepoErr(op, err)
}

func unauthorizedErr(op, message string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return fault.ErrUnauthorized.New(message).WithOp(op)
	}
	return mapRepoErr(op, err)
}

func forbiddenErr(op, message string) *fault.Fault {
	return fault.ErrForbidden.New(message).WithOp(op)
}

func mapRepoErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, eventstore.ErrCorruptEvent) {
		return fault.ErrInternal.Wrap(err, "corrupt data").WithOp(op)
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return fault.ErrConflict.Wrap(err, "data conflict").WithOp(op)
		case "23503", "23514":
			return fault.ErrUnprocessable.Wrap(err, "data constraint violated").WithOp(op)
		case "40001", "40P01", "55P03", "53300", "57P01":
			return unavailableErr(op, err)
		}
		if len(pgErr.Code) >= 2 && pgErr.Code[:2] == "08" {
			return unavailableErr(op, err)
		}
	}

	return internalErr(op, err)
}
