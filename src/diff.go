package src

import (
	"diff-redis/src/cursor"
	"github.com/gomodule/redigo/redis"
	"log"
	"sync"
	"time"
)

var (
	conf         Config
	scanKeysDone bool
	keysChan     = make(chan string, 65535)
	wg           = sync.WaitGroup{}
)

const TIMEOUT = 2 * time.Second

func Diff(config *Config) {
	defer close(keysChan)
	conf = *config
	wg.Add(1)
	go scanKeys()
	wg.Add(1)
	go examineKeys()
	wg.Wait()
}

func scanKeys() {
	defer wg.Done()

	conn, err := redis.Dial("tcp", conf.Source, defaultOpts(conf.SourcePassword)...)
	if err != nil {
		log.Fatalln("Connect Redis", conf.Source, "failed:", err)
	}
	defer conn.Close()
	defer func() {
		scanKeysDone = true
	}()

	_cursor := cursor.New(conn, conf.Count)
	log.Println("Cursor scanning")
	for _cursor.HasNext() {
		keys := _cursor.Next()
		log.Println("Scanned:", len(keys), "keys")
		for _, key := range keys {
			keysChan <- key
		}
	}
	log.Println("Cursor done scan")
}

func examineKeys() {
	defer wg.Done()

	conn, err := redis.Dial("tcp", conf.Target, defaultOpts(conf.TargetPassword)...)
	if err != nil {
		log.Fatalln("Connect Redis", conf.Target, "failed:", err)
	}
	defer conn.Close()

examining:
	for {
		select {
		case key := <-keysChan:
			reply, err := conn.Do("EXISTS", key)
			if err != nil {
				log.Println("Examine key exists error:", err)
			} else {
				exists, _ := redis.Int(reply, nil)
				if exists == 0 {
					log.Println("Key:", key, "miss")
				}
			}
		default:
			if scanKeysDone {
				log.Println("No further key scanning, break examining")
				break examining
			}
		}
	}
}

func defaultOpts(password string) []redis.DialOption {
	var options []redis.DialOption
	if password != "" {
		options = append(options, redis.DialPassword(password))
	}
	options = append(options, redis.DialConnectTimeout(TIMEOUT), redis.DialReadTimeout(TIMEOUT), redis.DialWriteTimeout(TIMEOUT))
	return options
}
