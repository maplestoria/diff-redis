package src

import (
	"github.com/gomodule/redigo/redis"
	"log"
	"regexp"
	"strconv"
	"strings"
)

type keyspace struct {
	db       int
	keys     int
	examined int
}

func getKeySpaces(conn redis.Conn) (result []keyspace) {
	reply, err := conn.Do("INFO", "KEYSPACE")
	if err != nil {
		log.Fatalln("get keyspace info error:", err)
	}

	resp, err := redis.String(reply, nil)
	if err != nil {
		log.Fatalln("parse keyspace reply error:", err)
	}

	for _, str := range strings.Split(resp, "\r\n") {
		pattern := "^db(\\d{1,2}):keys=(\\d+)"
		regex := regexp.MustCompile(pattern)

		submatch := regex.FindStringSubmatch(str)
		if submatch != nil {
			db, _ := strconv.Atoi(submatch[1])
			keys, _ := strconv.Atoi(submatch[2])
			result = append(result, keyspace{db: db, keys: keys})
		}
	}

	return result
}

func switchRedisDb(conn redis.Conn, db int) {
	log.Println("Switch Redis db to", db)

	reply, err := conn.Do("SELECT", db)
	if err != nil {
		log.Fatalln("Switch Redis db got error:", err.Error())
	}

	reply, _ = redis.String(reply, nil)
	if reply != "OK" {
		log.Fatalln("Can't switch Redis db")
	}
}
