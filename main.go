package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"time"

	// "fmt"
	"os"

	"log"

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
	eventsDir = "tmp/events_files/"
	timedelta int64 = 20000
)

type RDatabase struct {
	Client *redis.Client
}

type camEvent struct {
	Serial string `xml:"serial"`
	EventType string `xml:"eventType"`
	Ts time.Time `xml:"dateTime"`
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

	var err error
	rdbr, err := db.NewDatabase(*redisAddr)
	rdb = *rdbr
	if err != nil {
		log.Fatalf("Failed to connect to redis: %s", err.Error())
	}
	rdbr.Client.Ping(context.Background())

	_, t_err := taskScheduler.ScheduleWithCron(func(ctx context.Context) {
		log.Print("Scheduled Task With Cron")

		iter := rdb.Client.Scan(ctx, 0, "cam_*", 0).Iterator()
		events := `{"data": [` + "\n"
		for iter.Next(ctx) {
			val, err := rdb.Client.Get(ctx, iter.Val()).Result()
			if err != nil {
				log.Fatalf("Redis error %v", err)
			}
			events += val + ",\n"
		}
		if err := iter.Err(); err != nil {
			panic(err)
		}

		if events != `{"data": [` + "\n" {
			events += "]}"
			err := writeDayEventFile(events)
			if err != nil {
				log.Fatalf("Error writing to file: %v", err)
				panic(err)
			}
		}

		rdb.Client.FlushAll(ctx)
	}, "0 0 0 * * *")

	if t_err == nil {
		log.Print("Task has been scheduled successfully.")
	}

	h := eventHandler
	if *compress {
		h = fasthttp.CompressHandler(h)
	}

	if err := fasthttp.ListenAndServe(*addr, h); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func writeDayEventFile(data string) error {
	nowDate := time.Now().Format("2006-01-02")
	f, err := os.OpenFile(eventsDir + nowDate + ".json", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
		return err
	}
	_, err = f.WriteString(data)
	if err != nil {
		log.Fatalf("error writing to file: %v", err)
		return err
	}
	f.Close()
	return nil
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

	val, redisErr := rdb.Client.Get(ctx, "cam_" + req.Serial).Result()
	if errors.Is(redisErr, redis.Nil) {
		toSend = true
		rCam := redisCam{Serial: req.Serial, Ts: req.Ts.UnixMilli(), Count: 1}
		rdb.Client.Set(ctx, "cam_" + req.Serial, rCam.String(), 0)
		// log.Print("New cam ", rdb.Client.Get(ctx, req.Serial).Val())
	} else if redisErr != nil {
		log.Fatalf("Redis error %v", redisErr)
		err = redisErr
	} else {
		var rCam redisCam
		err = json.Unmarshal([]byte(val), &rCam)
		if err == nil && (rCam.Ts + timedelta < req.Ts.UnixMilli()) {
			toSend = true
			rCam.Ts = req.Ts.UnixMilli()
			rCam.Count += 1
			rdb.Client.Set(ctx, "cam_" + req.Serial, rCam.String(), 0)
			// log.Println("Cam ", rdb.Client.Get(ctx, req.Serial).Val())
		}
	}

	return toSend, err
}

func eventHandler(ctx *fasthttp.RequestCtx) {
	var req camEvent
	if err := xml.Unmarshal(ctx.PostBody(), &req); err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}

	toSend, err := doRedisSend(ctx, req)
	if err != nil {
		log.Panicln("Redis process error ", err)
	}

	if toSend {
		log.Println("Sending ", req.Serial, req.Ts.UnixMilli())
		go scheduleWrite(req.String() + "\n")
	} else {
		log.Println("Not sending ", req.Serial, req.Ts.UnixMilli())
	}
}
