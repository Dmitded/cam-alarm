package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	// "fmt"
	"os"

	"log"
	"time"

	"github.com/Dmitded/cam-alarm/db"
	"github.com/go-redis/redis/v8"
	"github.com/procyon-projects/chrono"
	"github.com/valyala/fasthttp"
)


var (
	addr = flag.String("addr", ":8080", "TCP address to listen to")
	redisAddr = flag.String("redis", "localhost:6379", "Redis address to connect to")
	compress = flag.Bool("compress", false, "Whether to enable transparent response compression")
	rdb db.Database
	taskScheduler = chrono.NewDefaultTaskScheduler()
)

const (
	pipeFile = "tmp/event"
	timedelta int64 = 20000
)

type RDatabase struct {
	Client *redis.Client
}

type camEvent struct {
	Serial string `json:"serial"`
	EventType string `json:"event_type"`
	Ts int64 `json:"ts"`
}

type redisCam struct {
	Serial string `json:"serial"`
	Ts int64 `json:"ts"`
	Count int `json:"count"`
}

func (e camEvent) String() string {
	out, err := json.Marshal(e)
	if err != nil {
		panic (err)
	}
	return string(out)
}

func (r redisCam) String() string {
	out, err := json.Marshal(&r)
	if err != nil {
		panic (err)
	}
	return string(out)
}


type PayLoad struct {
    Content string
}


func main() {
	flag.Parse()

	_, t_err := taskScheduler.ScheduleWithCron(func(ctx context.Context) {
		log.Print("Scheduled Task With Cron")
	}, "0 0 * * * *")

	if t_err == nil {
		log.Print("Task has been scheduled successfully.")
	}

	var err error
	rdbr, err := db.NewDatabase(*redisAddr)
	rdb = *rdbr
	if err != nil {
		log.Fatalf("Failed to connect to redis: %s", err.Error())
	}
	rdbr.Client.Ping(context.Background())

	h := eventHandler
	if *compress {
		h = fasthttp.CompressHandler(h)
	}

	if err := fasthttp.ListenAndServe(*addr, h); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func scheduleWrite(data string) {
	f, err := os.OpenFile(pipeFile, os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	_, err = f.WriteString(data)
	if err != nil {
		log.Fatalf("error writing to file: %v", err)
	}
	f.Close()
}

func doRedisSend(ctx *fasthttp.RequestCtx, req camEvent) (bool, error) {

	var toSend bool = false
	var err error = nil

	val, redisErr := rdb.Client.Get(ctx, req.Serial).Result()
	if errors.Is(redisErr, redis.Nil) {
		toSend = true
		rCam := redisCam{Serial: req.Serial, Ts: req.Ts, Count: 1}
		rdb.Client.Set(ctx, req.Serial, rCam.String(), 0)
		// log.Print("New cam ", rdb.Client.Get(ctx, req.Serial).Val())
	} else if redisErr != nil {
		log.Fatalf("Redis error %v", redisErr)
		err = redisErr
	} else {
		var rCam redisCam
		err = json.Unmarshal([]byte(val), &rCam)
		if err == nil && (rCam.Ts + timedelta < req.Ts) {
			toSend = true
			rCam.Ts = req.Ts
			rCam.Count += 1
			rdb.Client.Set(ctx, req.Serial, rCam.String(), 0)
			// log.Println("Cam ", rdb.Client.Get(ctx, req.Serial).Val())
		}
	}

	return toSend, err
}

func eventHandler(ctx *fasthttp.RequestCtx) {
	var req camEvent
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}
	req.Ts = time.Now().UnixMilli()

	toSend, err := doRedisSend(ctx, req)
	if err != nil {
		log.Panicln("Redis process error ", err)
	}

	if toSend {
		log.Println("Sending ", req.Ts)
		go scheduleWrite(req.String() + "\n")
	} else {
		log.Println("Not sending ", req.Ts)
	}
}
