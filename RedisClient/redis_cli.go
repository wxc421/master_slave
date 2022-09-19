package RedisClient

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
)

const (
	CRLF                   = "\r\n"
	redisArray             = '*'
	redisString            = '$'
	redisInt               = ':'
	redisSampleString      = '+'
	redisSampleErrorString = '-'
)

type RedisInfo struct {
	address string
	Conn    net.Conn
	Reader  *bufio.Reader

	bufRead []string
}

// 创建连接
func Client(address string, password string, db int) (client RedisInfo, err error) {
	fmt.Printf("连接Redis Server... , address: %s , password: %s , port: %d\n", address, password, db)
	client.address = address
	client.Conn, err = net.Dial("tcp", client.address)
	client.Reader = bufio.NewReader(client.Conn)

	// redis设置
	if "" != password {
		_, err = client.auth(password)
	}
	if 0 != db {
		_, err = client.Run("SELECT", strconv.Itoa(db))
	}

	return
}

// 发送数据
func (client *RedisInfo) connWrite(buf []byte) error {

	writeLen := 0
	for writeLen < len(buf) {
		n, err := client.Conn.Write(buf[writeLen:])

		if err != nil {
			return err
		}

		writeLen += n
	}

	return nil
}

// 接收数据
func (client *RedisInfo) RespRead() ([]string, error) {
	buf := make([]string, 0)
	var err error
	b, _, err := client.Reader.ReadLine()
	if err != nil || len(b) == 0 {
		return nil, err
	}
	switch b[0] {
	case redisArray:
		count, err := byteToInt(b[1:])
		if err != nil {
			return nil, err
		}
		for i := 0; i < count; i++ {
			resp, err := client.RespRead()
			if err != nil {
				return nil, err
			}
			buf = append(buf, resp...)
		}
	case redisSampleString:
		buf = append(buf, string(b[1:]))
	case redisInt:
		buf = append(buf, string(b[1:]))
	case redisSampleErrorString:
		err = errors.New(string(b[1:]))
	case redisString:
		n, _ := byteToInt(b[1:])
		if n <= 0 {
			buf = append(buf, "")
			break
		}
		newBuf := make([]byte, n)
		client.Reader.Read(newBuf)
		buf = append(buf, string(newBuf))

		crlf := make([]byte, 2)
		client.Reader.Read(crlf)
	}
	return buf, err
}

// 获取resp数组
func (client *RedisInfo) readArrayResp(count int) (buf []string, err error) {

	for i := 0; i < count; i++ {
		newBuf, _ := client.readSampleResp()
		buf = append(buf, newBuf...)
	}
	return
}

// 获取其他数据类型
func (client *RedisInfo) readSampleResp() (buf []string, err error) {
	newLine, _, _ := client.Reader.ReadLine()

	switch newLine[0] {
	case redisInt:
		buf = append(buf, string(newLine[1:]))
	case redisString:
		n, _ := byteToInt(newLine[1:])

		newBuf := make([]byte, n)
		client.Reader.Read(newBuf)
		buf = append(buf, string(newBuf))

		crlf := make([]byte, 2)
		client.Reader.Read(crlf)
	}

	return
}

// 字符转换为数字
func byteToInt(number []byte) (int, error) {
	var sum int
	x := 1
	for i := len(number) - 1; i >= 0; i-- {
		if number[i] < '0' || number[i] > '9' {
			return 0, errors.New("toNumberError")
		}
		sum += int(number[i]-'0') * x
		x = x * 10
	}

	return sum, nil
}

// run
func (client *RedisInfo) Run(com ...string) ([]string, error) {
	buf, _ := toRESP(com)
	client.connWrite(buf)
	return client.RespRead()
}

// auth
func (client *RedisInfo) auth(password string) ([]string, error) {
	var com []string
	com = append(com, "AUTH")
	com = append(com, password)

	buf, _ := toRESP(com)
	client.connWrite(buf)
	return client.RespRead()
}

// Set
func (client *RedisInfo) Set(key string, value string, seconds string, milliseconds string) ([]string, error) {

	fmt.Printf("set操作 , key: %s , val: %s , seconds: %s , milliseconds: %s \n", key, value, seconds, milliseconds)
	var com []string
	com = append(com, "SET")
	com = append(com, key)
	com = append(com, value)

	if seconds != "" {
		com = append(com, "EX")
		com = append(com, seconds)
	}

	if milliseconds != "" {
		com = append(com, "PX")
		com = append(com, milliseconds)
	}

	buf, _ := toRESP(com)
	fmt.Println("set待发送的报文: ", string(buf))

	client.connWrite(buf)
	return client.RespRead()
}

// HSet
func (client *RedisInfo) HSet(key string, field string, value string) ([]string, error) {
	var com []string

	com = append(com, "HSET")
	com = append(com, key)
	com = append(com, field)
	com = append(com, value)

	buf, _ := toRESP(com)

	client.connWrite(buf)
	return client.RespRead()
}

// HGelAll
func (client *RedisInfo) HGelAll(key string) ([]string, error) {
	var com []string

	com = append(com, "HGETALL")
	com = append(com, key)

	buf, _ := toRESP(com)

	client.connWrite(buf)
	return client.RespRead()
}

// HGet
func (client *RedisInfo) HGet(key string, field string) ([]string, error) {
	var com []string

	com = append(com, "HGET")
	com = append(com, key)
	com = append(com, field)

	buf, _ := toRESP(com)

	client.connWrite(buf)
	return client.RespRead()
}

// Get
func (client *RedisInfo) Get(key string) ([]string, error) {

	fmt.Printf("get操作 , key: %s\n", key)

	var com []string

	com = append(com, "GET")
	com = append(com, key)

	buf, _ := toRESP(com)
	fmt.Println("Get待发送报文: ", string(buf))

	client.connWrite(buf)
	return client.RespRead()
}

func (client *RedisInfo) Close() error {
	return client.Conn.Close()
}

// 将命令转化为RESP格式的报文
func toRESP(command []string) (resp []byte, err error) {
	arrayLen := len(command)
	if 0 == arrayLen {
		// 命令为空
		return
	}

	// 将长度写入到resp中
	resp = append(resp, redisArray)
	resp = append(resp, []byte(strconv.Itoa(arrayLen))...)
	resp = append(resp, []byte(CRLF)...)

	// 开始遍历数组
	for _, v := range command {
		// 获取数据长度
		vLen := len(v)
		resp = append(resp, redisString)
		resp = append(resp, []byte(strconv.Itoa(vLen))...)
		resp = append(resp, []byte(CRLF)...)
		resp = append(resp, []byte(v)...)
		resp = append(resp, []byte(CRLF)...)
	}
	return
}
