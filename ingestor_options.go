package bytearena

import "fmt"

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
