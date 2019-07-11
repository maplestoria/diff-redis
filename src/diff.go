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

type keyGroup struct {
	db   int
	keys []string
}

var (
	conf              *Config
	scanKeysDone      bool
	examiningKeysDone bool
	keysChan          = make(chan *keyGroup, 1024)
	missedKeys        = make(chan string, 65535)
	wg                = sync.WaitGroup{}
)

const TIMEOUT = 2 * time.Second

func Diff(config *Config) {
	defer close(keysChan)
	defer close(missedKeys)
	conf = config

	wg.Add(1)
	go scanKeys()
	wg.Add(1)
	go examineKeys()
	wg.Add(1)
	go writeToFile()
	printProgress()
	wg.Wait()
}

func scanKeys() {
	defer wg.Done()

	conn, err := redis.Dial("tcp", conf.Source, defaultRedisOpts(conf.SourcePassword)...)
	if err != nil {
		log.Fatalln("Connect Redis", conf.Source, "failed:", err)
	}
	defer conn.Close()
	defer func() {
		scanKeysDone = true
	}()

	keySpaces := getKeySpaces(conn)

	_cursor := cursor.New(conn, conf.Count)
	log.Println("Cursor scanning")
	for _, keyspace := range keySpaces {
		addKeyspace(keyspace)
		switchRedisDb(conn, keyspace.db)
		for _cursor.HasNext() {
			keys := _cursor.Next()
			group := keyGroup{db: keyspace.db, keys: keys}
			keysChan <- &group
		}
		_cursor.Reset()
	}
	log.Println("Cursor done scan")
}

func examineKeys() {
	defer wg.Done()

	conn, err := redis.Dial("tcp", conf.Target, defaultRedisOpts(conf.TargetPassword)...)
	if err != nil {
		log.Fatalln("Connect Redis", conf.Target, "failed:", err)
	}
	defer conn.Close()
	defer func() { examiningKeysDone = true }()

	currentDb := 0

examining:
	for {
		select {
		case keyGroup := <-keysChan:
			db := keyGroup.db
			if db != currentDb {
				switchRedisDb(conn, db)
				currentDb = db
			}
			for _, key := range keyGroup.keys {
				err := conn.Send("EXISTS", key)
				if err != nil {
					log.Fatalln("Send command to redis conn buffer error:", err)
				}
			}
			err := conn.Flush()
			if err != nil {
				log.Println("Flush redis conn buffer error:", err)
			} else {
				for index := range keyGroup.keys {
					reply, err := conn.Receive()
					if err != nil {
						log.Fatalln("Receive redis reply error:", err)
					} else {
						exists, _ := redis.Int(reply, nil)
						if exists == 0 {
							theKey := keyGroup.keys[index]
							missedKeys <- theKey
						}
					}
				}
				examined(currentDb, len(keyGroup.keys))
			}
		default:
			if scanKeysDone {
				log.Println("No further key scanning, break examining")
				break examining
			}
		}
	}
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
					log.Println("Missing key channel is empty, end writing")
					break writeFile
				} else {
					log.Println("Waiting for write key")
				}
			}
		}
	}
}
