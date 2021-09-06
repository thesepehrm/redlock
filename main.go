package main

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	redsync_goredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	// Config
	config = &Config{}
)

type Config struct {
	Redis struct {
		Host     string
		RoHost   string // readonly host
		Password string
		Database int
	}
}

func init() {
	logrus.SetFormatter(&logrus.TextFormatter{})
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		logrus.WithError(err).Fatal("Error reading config file")
	}
	err = viper.UnmarshalExact(&config)
	if err != nil {
		logrus.WithError(err).Fatal("Error unmarshalling config")
	}

}

func main() {

	// clusterSlots returns cluster slots information.
	clusterSlots := func(context.Context) ([]redis.ClusterSlot, error) {
		slots := []redis.ClusterSlot{
			// 1 master and 1 slave.
			{
				Start: 0,
				End:   16383,
				Nodes: []redis.ClusterNode{{
					Addr: config.Redis.Host, // master
				}, {
					Addr: config.Redis.RoHost, // 1st slave
				}},
			},
		}
		return slots, nil
	}

	cl := redis.NewClusterClient(&redis.ClusterOptions{
		Password:     config.Redis.Password,
		ReadOnly:     true,
		ClusterSlots: clusterSlots,
		NewClient: func(opt *redis.Options) *redis.Client {
			opt.DB = config.Redis.Database
			client := redis.NewClient(opt)
			return client
		},
	})

	defer cl.Close()

	err := cl.Ping(context.Background()).Err()
	if err != nil {
		logrus.WithError(err).Fatal("Error connecting to redis")
	}

	cl.ReloadState(context.Background())
	rs := redsync.New(redsync_goredis.NewPool(cl))

	mutex := rs.NewMutex("output", redsync.WithExpiry(time.Second*5))
	logrus.Info("Waiting")

	for {
		err := mutex.Lock()
		if err != nil {

			time.Sleep(time.Second * 1)
			continue
		}
		logrus.Info("Alright! It's my turn!")

		someWork()

		_, err = mutex.Unlock()
		if err != nil {
			logrus.WithError(err).Error("Error unlocking mutex")
		}

	}
}

func someWork() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	t := time.NewTicker(time.Second * 1)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			logrus.Info(time.Now().Format("2006-01-02 15:04:05"))
		case <-ctx.Done():
			logrus.Info("Done")
			return
		}
	}
}
