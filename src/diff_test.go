package src

import (
	"bufio"
	"github.com/gomodule/redigo/redis"
	"io"
	"log"
	"os"
	"testing"
)

func TestDiff_Target_Empty(t *testing.T) {
	log.Println("测试开始, 测试条件: Source插入5种类型数据, Target为空")
	source := "localhost:6379"
	target := "localhost:6479"
	output1 := "test.out_1"

	var keys = []string{"string_key", "hash_key", "list_key", "set_key", "sorted_set_key"}

	connS, _ := redis.Dial("tcp", source)
	connS.Do("SET", "string_key", "string_val")
	connS.Do("HSET", "hash_key", "hash_field_1", "field_val_1")
	connS.Do("LPUSH", "list_key", "list_val_1")
	connS.Do("SADD", "set_key", "set_val_1")
	connS.Do("ZADD", "sorted_set_key", 1.0, "sorted_set_val_1")

	connT, _ := redis.Dial("tcp", target)
	connT.Do("FLUSHALL")
	connT.Close()

	config := Config{Source: source, Target: target, Output: output1}
	Diff(&config)

	for _, key := range keys {
		connS.Do("DELETE", key)
	}
	connS.Close()

	file, err := os.OpenFile(output1, os.O_RDONLY, 0666)
	if err != nil {
		t.Fatalf("Open result file [%s] error:\n", err.Error())
	}
	reader := bufio.NewReader(file)

	keyMap := map[string]string{}

	for {
		line, _, err := reader.ReadLine()
		if err != nil {
			if err != io.EOF {
				t.Error("Read file error:", err)
			} else {
				break
			}
		} else {
			key := string(line)
			log.Println("Read key:", key)
			keyMap[key] = key
		}
	}
	file.Close()
	defer os.Remove(output1)

	for _, key := range keys {
		s := keyMap[key]
		if s == "" {
			t.Fatalf("Test failed, key [%s] should exists\n", key)
		}
	}
}
