package cluster

import (
	"context"
	"errors"
	pool "github.com/jolestar/go-commons-pool/v2"
	"godis-instruction/config"
	"godis-instruction/lib/utils"
	"godis-instruction/redis/client"
)

type connectionFactory struct {
	peer string
}

func (f *connectionFactory) MakeObject(_ context.Context) (*pool.PooledObject, error) {
	cli, err := client.NewClient(f.peer)
	if err != nil {
		return nil, err
	}
	cli.Start()
	password := config.Properties.RequirePass
	if password != "" {
		cli.Send(utils.StringsToLine("AUTH", password))
	}
	return pool.NewPooledObject(cli), nil
}

func (f *connectionFactory) DestroyObject(_ context.Context, obj *pool.PooledObject) error {
	cli, ok := obj.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	cli.Close()
	return nil
}

func (f *connectionFactory) ValidateObject(_ context.Context, _ *pool.PooledObject) bool {
	return true
}

func (f *connectionFactory) ActivateObject(_ context.Context, _ *pool.PooledObject) error {
	return nil
}

func (f *connectionFactory) PassivateObject(_ context.Context, _ *pool.PooledObject) error {
	return nil
}
