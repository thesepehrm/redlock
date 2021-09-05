package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/redigo"
	"github.com/gomodule/redigo/redis"
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
		Port     int
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

	// Create a pool of redis connections.
	pool := redigo.NewPool(&redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", fmt.Sprintf("%s:%d", config.Redis.Host, config.Redis.Port), redis.DialPassword(config.Redis.Password), redis.DialDatabase(config.Redis.Database))
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	})

	rs := redsync.New(pool)

	mutex := rs.NewMutex("output", redsync.WithExpiry(time.Second*5))
	logrus.Info("Waiting")

	for {

		err := mutex.Lock()
		if err != nil {
			time.Sleep(time.Second * 1)
			continue
		}
		logrus.Info("Alright! It's my turn!")
		if err != nil {
			logrus.WithError(err).Error("Error extending lock")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		timer(ctx)
		cancel()
		_, err = mutex.Unlock()
		if err != nil {
			logrus.WithError(err).Error("Error unlocking mutex")
		}

	}
}

func timer(ctx context.Context) {
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
