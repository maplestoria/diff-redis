package src

import "github.com/gomodule/redigo/redis"

type Config struct {
	Source         string
	SourcePassword string
	Target         string
	TargetPassword string
	Output         string
	Count          int
}

// 默认的Redis连接配置
func defaultRedisOpts(password string) []redis.DialOption {
	var options []redis.DialOption
	if password != "" {
		options = append(options, redis.DialPassword(password))
	}
	options = append(options, redis.DialConnectTimeout(TIMEOUT), redis.DialReadTimeout(TIMEOUT), redis.DialWriteTimeout(TIMEOUT))
	return options
}
