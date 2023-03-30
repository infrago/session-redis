package session_redis

import (
	"encoding/base64"
	"errors"
	"sync"
	"time"

	. "github.com/infrago/base"
	"github.com/infrago/log"
	"github.com/infrago/session"
	"github.com/infrago/util"

	"github.com/gomodule/redigo/redis"
)

//-------------------- redisBase begin -------------------------

var (
	errInvalidCacheConnection = errors.New("Invalid session connection.")
	errEmptyData              = errors.New("Empty session data.")
)

type (
	redisDriver  struct{}
	redisConnect struct {
		mutex   sync.RWMutex
		actives int64

		instance *session.Instance
		setting  redisSetting

		client *redis.Pool
	}
	redisSetting struct {
		Server   string //服务器地址，ip:端口
		Password string //服务器auth密码
		Database string //数据库
		Expiry   time.Duration

		Idle    int //最大空闲连接
		Active  int //最大激活连接，同时最大并发
		Timeout time.Duration
	}
)

// 连接
func (driver *redisDriver) Connect(inst *session.Instance) (session.Connect, error) {
	setting := redisSetting{
		Server: "127.0.0.1:6379", Password: "", Database: "",
		Idle: 30, Active: 100, Timeout: 240,
	}

	if vv, ok := inst.Setting["server"].(string); ok && vv != "" {
		setting.Server = vv
	}
	if vv, ok := inst.Setting["password"].(string); ok && vv != "" {
		setting.Password = vv
	}

	//数据库，redis的0-16号
	if v, ok := inst.Setting["database"].(string); ok {
		setting.Database = v
	}

	if vv, ok := inst.Setting["idle"].(int64); ok && vv > 0 {
		setting.Idle = int(vv)
	}
	if vv, ok := inst.Setting["active"].(int64); ok && vv > 0 {
		setting.Active = int(vv)
	}
	if vv, ok := inst.Setting["timeout"].(int64); ok && vv > 0 {
		setting.Timeout = time.Second * time.Duration(vv)
	}
	if vv, ok := inst.Setting["timeout"].(string); ok && vv != "" {
		td, err := util.ParseDuration(vv)
		if err == nil {
			setting.Timeout = td
		}
	}

	return &redisConnect{
		instance: inst, setting: setting,
	}, nil
}

// 打开连接
func (this *redisConnect) Open() error {
	this.client = &redis.Pool{
		MaxIdle: this.setting.Idle, MaxActive: this.setting.Active, IdleTimeout: this.setting.Timeout,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", this.setting.Server)
			if err != nil {
				log.Warning("session.redis.dial", err)
				return nil, err
			}

			//如果有验证
			if this.setting.Password != "" {
				if _, err := c.Do("AUTH", this.setting.Password); err != nil {
					c.Close()
					log.Warning("session.redis.auth", err)
					return nil, err
				}
			}
			//如果指定库
			if this.setting.Database != "" {
				if _, err := c.Do("SELECT", this.setting.Database); err != nil {
					c.Close()
					log.Warning("session.redis.select", err)
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	//打开一个试一下
	conn := this.client.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return err
	}
	return nil
}

// 关闭连接
func (this *redisConnect) Close() error {
	if this.client != nil {
		if err := this.client.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 查询会话，
func (this *redisConnect) Exists(id string) (bool, error) {
	if this.client == nil {
		return false, errInvalidCacheConnection
	}

	conn := this.client.Get()
	defer conn.Close()

	exists, err := redis.Int(conn.Do("EXISTS", id))
	if err != nil {
		log.Warning("session.redis.exists", err)
		return false, err
	}

	if exists > 0 {
		return true, nil
	}

	return false, nil
}

// 查询会话
func (this *redisConnect) Read(id string) ([]byte, error) {
	if this.client == nil {
		return nil, errInvalidCacheConnection
	}

	conn := this.client.Get()
	defer conn.Close()

	value, err := redis.String(conn.Do("GET", id))
	if err != nil && err != redis.ErrNil {
		log.Warning("session.redis.read", err)
		return nil, err
	}
	if value == "" {
		return nil, nil
	}

	return base64.StdEncoding.DecodeString(value)
}

// 更新会话
func (this *redisConnect) Write(id string, data []byte, expiry time.Duration) error {
	if this.client == nil {
		return errInvalidCacheConnection
	}

	value := base64.StdEncoding.EncodeToString(data)
	if value == "" {
		return errEmptyData
	}

	conn := this.client.Get()
	defer conn.Close()

	args := []Any{
		id, value,
	}
	if expiry > 0 {
		args = append(args, "EX", expiry.Seconds())
	}

	_, err := conn.Do("SET", args...)
	if err != nil {
		log.Warning("session.redis.write", err)
		return err
	}

	return nil
}

// 删除会话
func (this *redisConnect) Delete(id string) error {
	if this.client == nil {
		return errInvalidCacheConnection
	}

	conn := this.client.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", id)
	if err != nil {
		return err
	}
	return nil
}

func (this *redisConnect) Clear(prefix string) error {
	if this.client == nil {
		return errInvalidCacheConnection
	}

	conn := this.client.Get()
	defer conn.Close()

	ids, err := this.Keys(prefix)
	if err != nil {
		return err
	}

	for _, id := range ids {
		_, err := conn.Do("DEL", id)
		if err != nil {
			return err
		}
	}

	return nil
}
func (this *redisConnect) Keys(prefix string) ([]string, error) {
	if this.client == nil {
		return nil, errInvalidCacheConnection
	}

	conn := this.client.Get()
	defer conn.Close()

	ids := []string{}

	alls, _ := redis.Strings(conn.Do("KEYS", prefix+"*"))
	for _, id := range alls {
		ids = append(ids, id)
	}

	return ids, nil
}

//-------------------- redisBase end -------------------------
