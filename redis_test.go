package golang_redis

import (
	"context"
	"fmt"
	"strconv"
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

func TestSortedTest(t *testing.T) {
	client.ZAdd(ctx, "scores", redis.Z{
		Score:  100,
		Member: "Muhammad",
	})
	client.ZAdd(ctx, "scores", redis.Z{
		Score:  85,
		Member: "Irfan",
	})
	client.ZAdd(ctx, "scores", redis.Z{
		Score:  95,
		Member: "Zidni",
	})

	assert.Equal(t, []string{"Irfan", "Zidni", "Muhammad"}, client.ZRange(ctx, "scores", 0, -1).Val())

	assert.Equal(t, "Muhammad", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Zidni", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Irfan", client.ZPopMax(ctx, "scores").Val()[0].Member)
}

func TestHas(t *testing.T) {
	client.HSet(ctx, "user:1", "id", "1")
	client.HSet(ctx, "user:1", "name", "Irfan")
	client.HSet(ctx, "user:1", "email", "1")
}

func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko A",
		Latitude:  -6.968240641703725,
		Longitude: 110.428435652178,
	})
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko B",
		Latitude:  -6.969156505365211,
		Longitude: 110.43095692853184,
	})

	distance := client.GeoDist(ctx, "sellers", "Toko A", "Toko B", "km").Val()
	assert.Equal(t, 0.2964, distance)

	sellers := client.GeoSearch(ctx, "sellers", &redis.GeoSearchQuery{
		Latitude:   -6.967909459897636,
		Longitude:  110.42654528036203,
		Radius:     5,
		RadiusUnit: "km",
	}).Val()

	assert.Equal(t, []string{"Toko A", "Toko B"}, sellers)
}

func TestHyperLogLog(t *testing.T) {
	client.PFAdd(ctx, "visitors", "muhammad", "irfan", "zidni")
	client.PFAdd(ctx, "visitors", "muhammad", "budi", "joko")
	client.PFAdd(ctx, "visitors", "ruli", "budi", "joko")

	toal := client.PFCount(ctx, "visitors").Val()
	assert.Equal(t, int64(6), toal)
}

func TestPipeline(t *testing.T) {
	_, err := client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "Irfan", 5*time.Second)
		pipeliner.SetEx(ctx, "address", "Indonesia", 5*time.Second)
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, "Irfan", client.Get(ctx, "name").Val())
	assert.Equal(t, "Indonesia", client.Get(ctx, "address").Val())
}

func TestTransaction(t *testing.T) {
	_, err := client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "name", "Joko", 5*time.Second)
		pipeliner.SetEx(ctx, "address", "Cirebon", 5*time.Second)
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, "Joko", client.Get(ctx, "name").Val())
	assert.Equal(t, "Cirebon", client.Get(ctx, "address").Val())
}

func TestPublishStream(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: "members",
			Values: map[string]interface{}{
				"name":    "Irfan",
				"address": "Indonesia",
			},
		}).Err()
		assert.Nil(t, err)
	}
}

func TestCreatConsumerGroup(t *testing.T) {
	client.XGroupCreate(ctx, "members", "group-1", "0")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-1")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-2")
}

func TestConsumerStream(t *testing.T) {
	streams := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "group-1",
		Consumer: "consumer-1",
		Streams:  []string{"members", ">"},
		Count:    2,
		Block:    5 * time.Second,
	}).Val()

	for _, stream := range streams {
		for _, message := range stream.Messages {
			fmt.Println(message.ID)
			fmt.Println(message.Values)
		}
	}
}

func TestSubscribePubSub(t *testing.T) {
	subscriber := client.Subscribe(ctx, "channel-1")
	defer subscriber.Close()

	for i := 0; i < 10; i++ {
		message, err := subscriber.ReceiveMessage(ctx)
		assert.Nil(t, err)
		fmt.Println(message.Payload)
	}
}

func TestPublishPubSub(t *testing.T) {
	for i := 0; i < 10; i++ {
		err:=client.Publish(ctx, "channel-1", "Hello" + strconv.Itoa(i))
		assert.Nil(t, err)
	}
}
