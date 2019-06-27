package src

import (
	"bufio"
	"diff-redis/src/cursor"
	"github.com/gomodule/redigo/redis"
	"log"
	"os"
	"sync"
	"time"
)

var (
	conf              Config
	scanKeysDone      bool
	examiningKeysDone bool
	keysChan          = make(chan []string, 1024)
	missedKeys        = make(chan string, 65535)
	wg                = sync.WaitGroup{}
)

const TIMEOUT = 2 * time.Second

func Diff(config *Config) {
	defer close(keysChan)
	defer close(missedKeys)
	conf = *config
	wg.Add(1)
	go scanKeys()
	wg.Add(1)
	go examineKeys()
	wg.Add(1)
	go writeToFile()
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
		keysChan <- keys
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
	defer func() { examiningKeysDone = true }()
examining:
	for {
		select {
		case keys := <-keysChan:
			for _, key := range keys {
				err := conn.Send("EXISTS", key)
				if err != nil {
					log.Fatalln("Send command to redis conn buffer error:", err)
				}
			}
			err := conn.Flush()
			if err != nil {
				log.Println("Flush redis conn buffer error:", err)
			} else {
				for index := range keys {
					reply, err := conn.Receive()
					if err != nil {
						log.Println("Receive redis reply error:", err)
					} else {
						exists, _ := redis.Int(reply, nil)
						if exists == 0 {
							log.Println("key", keys[index], "missed in target")
							missedKeys <- keys[index]
						}
					}
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

func writeToFile() {
	defer wg.Done()
	file, err := os.Create(conf.Output)
	if err != nil {
		log.Fatalln("Open output file error:", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer func() {
		log.Println("Exiting. Flush all buffered keys...")
		writer.Flush()
	}()

writeFile:
	for {
		select {
		case missedKey := <-missedKeys:
			key := missedKey + "\n"
			available := writer.Available()
			if available < len(key) {
				if err := writer.Flush(); err != nil {
					log.Fatalln("Writer flush got error:", err)
				}
			}
			if _, err := writer.WriteString(key); err != nil {
				log.Println("Writer writing got error:", err)
			}
		default:
			if examiningKeysDone && scanKeysDone {
				if len(missedKeys) == 0 {
					log.Println("Channel is empty, end writing")
					break writeFile
				} else {
					log.Println("Waiting for ")
				}
			}
		}
	}
}
