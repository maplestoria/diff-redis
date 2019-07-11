package src

import (
	"fmt"
	"log"
	"sync"
	"time"
)

var counter = sync.Map{}

func addKeyspace(ks keyspace) {
	counter.Store(ks.db, ks)
}

func examined(db int, size int) {
	if value, ok := counter.Load(db); ok {
		if ks, ok := value.(keyspace); ok {
			ks.examined += size
			counter.Store(db, ks)
		}
	}
}

func printProgress() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for range ticker.C {
			calcProgress()
		}
	}()
}

func calcProgress() {
	counter.Range(func(key, value interface{}) bool {
		if ks, ok := value.(keyspace); ok {
			db := ks.db
			keys := ks.keys
			examined := ks.examined

			progress := (float64(examined) / float64(keys)) * 100
			percent := fmt.Sprintf("%.2f%s", progress, "%")

			log.Println("db:", db, "keys:", keys, "examined:", examined, "progress:", percent)
		}
		return false
	})
}
