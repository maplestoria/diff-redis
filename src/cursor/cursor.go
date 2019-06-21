package cursor

import (
	"github.com/gomodule/redigo/redis"
	"log"
)

// None thread safe
type Cursor struct {
	conn   redis.Conn
	count  int
	done   bool
	cursor int
}

func New(conn redis.Conn, count int) *Cursor {
	cursor := &Cursor{conn: conn, count: count, done: false, cursor: 0}
	return cursor
}

func (iterator *Cursor) Next() []string {
	if iterator.done {
		return nil
	}

	conn := iterator.conn
	reply, err := conn.Do("SCAN", iterator.cursor, "COUNT", iterator.count)
	if err != nil {
		log.Fatalln("Cursor do scan failed", err)
	}

	values, err := redis.Values(reply, nil)
	if err != nil {
		log.Fatalln("Cursor's reply is not []interface{}, parse failed:", err)
	}

	iterator.cursor, _ = redis.Int(values[0], nil)
	keys, _ := redis.Strings(values[1], nil)
	if iterator.cursor == 0 {
		iterator.done = true
	}
	return keys
}
