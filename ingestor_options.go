package bytearena

import (
	"errors"
	"fmt"
	"io"
)

type Options func(*Ingestor) error

func WithSealPercentage(percentage uint32) Options {
	return func(i *Ingestor) error {
		if percentage < 20 || percentage > 99 {
			return fmt.Errorf(
				"seal percentage must be in (20,99], got %d",
				percentage,
			)
		}

		i.arenaSealPercentage = percentage

		return nil
	}
}

func WithTickMiliseconds(interval uint16) Options {
	return func(i *Ingestor) error {
		if interval == 0 {
			return errors.New("tick value cannot be zero")
		}

		i.milisecondsTickInterval = interval

		return nil
	}
}

func WithUnblockMiliseconds(interval uint16) Options {
	return func(i *Ingestor) error {
		if interval == 0 {
			return errors.New("unblock flush value cannot be zero")
		}

		i.milisecondsUnblock = interval

		return nil
	}
}

func WithTelemetry() Options {
	return func(i *Ingestor) error {
		i.withTelemetry = true

		return nil
	}
}

func WithTelemetryWriter(w io.Writer) Options {
	return func(i *Ingestor) error {
		i.writerTelemetry = w

		return nil
	}
}

func WithErrorsWriter(w io.Writer) Options {
	return func(i *Ingestor) error {
		i.writerErrors = w

		return nil
	}
}
