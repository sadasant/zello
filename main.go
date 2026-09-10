package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sadasant/zello/internal/cli"
	"github.com/sadasant/zello/internal/config"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	paths, err := config.UserPaths()
	if err == nil {
		err = cli.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, paths)
	}
	if err != nil {
		if ctx.Err() != nil {
			os.Exit(130)
		}
		if !errors.Is(err, cli.ErrDisconnected) {
			fmt.Fprintln(os.Stderr, "zello:", err)
		}
		os.Exit(1)
	}
}
