package cursor

import (
	"github.com/gomodule/redigo/redis"
	"log"
)

type Cursor struct {
	conn    redis.Conn
	count   int
	hasDone bool
	next    int
}

func New(conn redis.Conn, count int) *Cursor {
	if count <= 0 {
		count = 256
	}
	return &Cursor{conn: conn, count: count, hasDone: false, next: 0}
}

func (cursor *Cursor) Next() []string {
	if !cursor.HasNext() {
		return nil
	}

	conn := cursor.conn
	reply, err := conn.Do("SCAN", cursor.next, "COUNT", cursor.count)
	if err != nil {
		log.Fatalln("Cursor scan failed:", err)
	}

	values, err := redis.Values(reply, nil)
	if err != nil {
		log.Fatalln("Cursor's reply is not []interface{}, parse failed:", err)
	}

	cursor.next, _ = redis.Int(values[0], nil)
	keys, _ := redis.Strings(values[1], nil)
	if cursor.next == 0 {
		cursor.hasDone = true
	}
	return keys
}

func (cursor *Cursor) HasNext() bool {
	return !cursor.hasDone
}
