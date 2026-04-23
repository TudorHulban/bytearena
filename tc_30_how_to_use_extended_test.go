package bytearena_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena"
)

func TestManual(t *testing.T) {
	// t.Skip("manual only")

	writer := os.Stdout

	ingestor, errCrIngestor := bytearena.NewIngestor(
		bytearena.Size100K(),
		writer,
	)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	defer func() {
		cancel()
		<-chIngestionEnd
	}()

	app := fiber.New()

	app.Get(
		"/",
		func(c fiber.Ctx) error {
			payload := "xxxxxxxxxxxxxxxxxxxx\n"

			var response string

			bytesWritten, errWrite := ingestor.Write([]byte(payload))
			if errWrite != nil {
				response = fmt.Sprintf(
					"there was error writing: %s",
					errWrite.Error(),
				)
			} else {
				response = fmt.Sprintf(
					"wrote: %d bytes with no error",
					bytesWritten,
				)
			}

			return c.SendString(
				response,
			)
		},
	)

	log.Fatal(app.Listen(":3000"))
}
