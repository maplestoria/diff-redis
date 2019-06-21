package src

import (
	"diff-redis/src/cursor"
	"github.com/gomodule/redigo/redis"
	"log"
	"time"
)

var (
	conf    Config
	done    = make(chan bool)
	timeout = 2 * time.Second
)

func Diff(config *Config) {
	conf = *config
	scanKeys()
}

func scanKeys() {
	var options []redis.DialOption
	if conf.SourcePassword != "" {
		options = append(options, redis.DialPassword(conf.SourcePassword))
	}
	options = append(options, redis.DialConnectTimeout(timeout), redis.DialReadTimeout(timeout), redis.DialWriteTimeout(timeout))

	conn, err := redis.Dial("tcp", conf.Source, options...)
	if err != nil {
		log.Fatalln("Connect Redis", conf.Source, "failed:", err)
	}
	defer conn.Close()

	_cursor := cursor.New(conn, conf.Count)
	go func() {
		log.Println("Cursor scanning")
		for {
			keys := _cursor.Next()
			if len(keys) == 0 {
				break
			} else {
				log.Println("Scanned:", keys)
			}
		}
		log.Println("Cursor done scan")
		done <- true
	}()
	hasDone := <-done
	if hasDone {
		log.Println("Scan keys has done")
	}
}
