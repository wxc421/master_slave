package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"master_slave/RedisClient"
	"net"
	"strconv"
	"strings"
	"sync"
)

var toSlaveCommand map[string]bool

var (
	redisMasterConn = "192.168.40.132:6379"
	redisSlaveConn  = "192.168.40.132:6380"
)

func init() {
	getSlaveCommand()
}

func getSlaveCommand() {
	// 维护从库执行的命令
	toSlaveCommand = make(map[string]bool, 0)

	// key
	toSlaveCommand["GET"] = true

	// list
	toSlaveCommand["LLEN"] = true
	toSlaveCommand["LRANGE"] = true

	// hash
	toSlaveCommand["HEXISTS"] = true
	toSlaveCommand["HGET"] = true
	toSlaveCommand["HLEN"] = true
	toSlaveCommand["HLEN"] = true
}

type ZConn struct {
	mcc    *RedisClient.RedisInfo
	scc    *RedisClient.RedisInfo
	conn   net.Conn
	reader *bufio.Reader
	once   sync.Once
	mutex  sync.Mutex
}

func (c *ZConn) Close() {
	c.once.Do(func() {
		c.mcc.Close()
		c.scc.Close()
		c.conn.Close()
	})
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:8889")
	if err != nil {
		panic(err)
	}
	fmt.Println("服务器已经启动好了，监听地址: 0.0.0.0:8889")
	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("accept error ", err)
			continue
		}
		fmt.Println(c.RemoteAddr(), " 连接到Redis中间件")
		// 为连接上来的每个conn分配主从redis conn
		mcc, err := RedisClient.Client(redisMasterConn, "", 0)
		if err != nil {
			fmt.Println("dial redis master conn error: ", err)
			continue
		}
		fmt.Println(c.RemoteAddr(), " 连接到Redis主库")
		scc, err := RedisClient.Client(redisSlaveConn, "", 0)
		if err != nil {
			fmt.Println("dial redis slave conn error: ", err)
			continue
		}
		fmt.Println(c.RemoteAddr(), " 连接到Redis从库")
		zconn := &ZConn{
			mcc:    &mcc,
			scc:    &scc,
			conn:   c,
			reader: bufio.NewReader(c),
		}

		go worker(zconn)
	}
}

// worker
func worker(zconn *ZConn) {
	defer func() {
		zconn.Close()
	}()

	go func() {
		defer zconn.Close()
		buffer := new(bytes.Buffer)
		zconn.scc.Reader = bufio.NewReader(io.TeeReader(zconn.scc.Reader, buffer))
		for {
			buffer.Reset()
			if _, err := zconn.scc.RespRead(); err != nil {
				return
			}
			nn := 0
			b := buffer.Bytes()
			// write data to conn
			for nn < buffer.Len() {
				zconn.mutex.Lock()
				n, err := zconn.conn.Write(b[nn:])
				zconn.mutex.Unlock()
				if err != nil {
					return
				}
				nn += n
			}
		}
	}()

	go func() {
		defer zconn.Close()
		buffer := new(bytes.Buffer)
		zconn.mcc.Reader = bufio.NewReader(io.TeeReader(zconn.mcc.Reader, buffer))
		for {
			buffer.Reset()
			if _, err := zconn.mcc.RespRead(); err != nil {
				return
			}
			nn := 0
			b := buffer.Bytes()
			// write data to conn
			for nn < buffer.Len() {
				zconn.mutex.Lock()
				n, err := zconn.conn.Write(b[nn:])
				zconn.mutex.Unlock()
				if err != nil {
					return
				}
				nn += n
			}
		}
	}()

	buffer := new(bytes.Buffer)
	reader := bufio.NewReader(io.TeeReader(zconn.conn, buffer))
	for {
		ss := make([]string, 0)
		buffer.Reset()
		if err := toArgs(reader, &ss); err != nil {
			fmt.Println(err)
			return
		}
		_, ok := toSlaveCommand[strings.ToTitle(ss[0])]
		if !ok {
			fmt.Println(zconn.conn.RemoteAddr(), "获取命令: ", ss, "转发到从库")
			if err := toAgent(zconn.scc.Conn, buffer, int64(buffer.Len())); err != nil {
				return
			}
		} else {
			fmt.Println(zconn.conn.RemoteAddr(), "获取命令: ", ss, "转发到主库")
			if err := toAgent(zconn.mcc.Conn, buffer, int64(buffer.Len())); err != nil {
				return
			}
		}
	}
}

// 将RESP转换为[]interface 类型
func toArgs(reader *bufio.Reader, ss *[]string) error {
	line, _, _ := reader.ReadLine()

	if len(line) < 2 {
		return errors.New("获取报文有误")
	}

	switch line[0] {
	case '*':
		n, err := byteToInt(line[1:])
		if err != nil {
			return errors.New("转为数字错误")
		}
		// 递归
		for i := 0; i < int(n); i++ {
			if err := toArgs(reader, ss); err != nil {
				return err
			}
		}
	case '$':
		n, err := byteToInt(line[1:])
		if n <= 0 {
			*ss = append(*ss, "")
			return nil
		}
		if err != nil {
			return errors.New("转为数字错误")
		}
		buf := make([]byte, n)
		_, err = reader.Read(buf)
		if err != nil {
			return errors.New("获取命令失败")
		}

		*ss = append(*ss, string(buf))
		crlf := make([]byte, 2)
		reader.Read(crlf)
		if string(crlf) != "\r\n" {
			return errors.New("获取命令失败")
		}
	}
	return nil
}

// []byte 转为 int
// 例如 []byte(string("11")) 转为 int类型的 11
func byteToInt(number []byte) (int64, error) {
	return strconv.ParseInt(string(number), 10, 64)
}

// 将[]interface 类型转换为RESP数据
func genArrayArgs(ss []interface{}) (respMsg []byte) {
	bodyLen := len(ss)

	respMsg = append(respMsg, []byte(string("*"))...)
	respMsg = append(respMsg, []byte(strconv.Itoa((bodyLen))+"\r\n")...)

	for i := 0; i < len(ss); i++ {
		switch ss[i].(type) {
		case int:
			respMsg = append(respMsg, []byte(string(":"))...)
			x := ss[i].(int)
			respMsg = append(respMsg, []byte(strconv.Itoa((x))+"\r\n")...)
		case string:
			respMsg = append(respMsg, []byte(string("$"))...)
			str1 := ss[i].(string)
			respMsg = append(respMsg, []byte(string(strconv.Itoa((len(str1)))+"\r\n"))...)
			respMsg = append(respMsg, []byte(string(str1+"\r\n"))...)
		}
	}
	return
}

// 转发信息到真实的服务器
func toAgent(dst io.Writer, src io.Reader, n int64) error {
	for n > 0 {
		nn, err := io.CopyN(dst, src, n)
		if err != nil && err != io.EOF {
			return err
		}
		n -= nn
	}
	return nil
}
