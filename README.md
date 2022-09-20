# redis中间件实现

## 参考博客
https://juejin.cn/post/7140612873403203620

https://juejin.cn/post/7136581475738058760

## 实现功能
客户端连接中间件,中间件解析RESP命令并转发到主库或者从库，再将结果返回

## 实现架构

![image-20220920095437376](README.assets/image-20220920095437376.png)
