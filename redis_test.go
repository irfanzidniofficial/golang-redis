package golang_redis

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var client = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	DB:   0,
})

func TestConnect(t *testing.T) {
	assert.NotNil(t, client)

	err := client.Close()
	assert.Nil(t, err)
}

var ctx = context.Background()

func TestPing(t *testing.T) {
	result, err := client.Ping(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	client.SetEx(ctx, "name", "Irfan Zidni", 3*time.Second)

	result, err := client.Get(ctx, "name").Result()
	assert.Nil(t, err)
	assert.Equal(t, "Irfan Zidni", result)

	time.Sleep(5 * time.Second)
	result, err = client.Get(ctx, "name").Result()
	assert.NotNil(t, err)
}

func TestList(t *testing.T) {
	client.RPush(ctx, "names", "Muhammad")
	client.RPush(ctx, "names", "Irfan")
	client.RPush(ctx, "names", "Zidni")

	assert.Equal(t, "Muhammad", client.LPop(ctx, "names").Val())
	assert.Equal(t, "Irfan", client.LPop(ctx, "names").Val())
	assert.Equal(t, "Zidni", client.LPop(ctx, "names").Val())

	client.Del(ctx, "names")
}

func TestSet(t *testing.T) {
	client.SAdd(ctx, "students", "Muhammad")
	client.SAdd(ctx, "students", "Muhammad")
	client.SAdd(ctx, "students", "Irfan")
	client.SAdd(ctx, "students", "Irfan")
	client.SAdd(ctx, "students", "Zidni")
	client.SAdd(ctx, "students", "Zidni")

	assert.Equal(t, int64(3), client.SCard(ctx, "students").Val())
	assert.Equal(t, []string{"Muhammad", "Irfan", "Zidni"}, client.SMembers(ctx, "students").Val())
}
