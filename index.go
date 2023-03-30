package session_redis

import (
	"github.com/infrago/infra"
	"github.com/infrago/session"
)

func Driver() session.Driver {
	return &redisDriver{}
}

func init() {
	infra.Register("redis", Driver())
}
