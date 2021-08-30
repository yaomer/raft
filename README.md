### cluster

假如你的配置文件中`cluster nodes`的配置是这样的，有5个节点
```
# cluster nodes
node 127.0.0.1 8000
node 127.0.0.1 8001
node 127.0.0.1 8002
node 127.0.0.1 8003
node 127.0.0.1 8004
```
那么，你就可以打开5个终端，在每个终端分别执行
```
$ ./serv 8000
$ ./serv 8001
$ ./serv 8002
$ ./serv 8003
$ ./serv 8004
```
这样就可以将cluster跑起来了，然后你可以尝试kill掉其中的任何一个节点(leader or follower)，
或者将被kill掉的节点重新拉起，来观察cluster的工作过程。

但请注意，一旦剩余节点数量小于3，cluster将不能正常工作。

### client

+ client每次开始运行时会随机在cluster nodes中挑选一个
+ 如果连接超时，就再随机选一个
+ 如果连接上的不是leader，那么leader会发送`<host>host\r\n`来帮client重定向到leader
+ 如果连接上的是leader，那么就可以正常通信了，消息格式为`<user>msg\r\n`。
我在raft上运行了一个只支持`set`和`get`的简单的K-V服务，所以服务端的响应分为3种：`+ok\r\n`，`-error\r\n`，`$reply\r\n`
+ 如果之后leader崩溃了，就重复上述步骤

#### 下面是一些运行截图
![](https://github.com/yaomer/pictures/blob/master/raft-cli.png?raw=true)

![](https://github.com/yaomer/pictures/blob/master/raft-s0.png?raw=true)

![](https://github.com/yaomer/pictures/blob/master/raft-s1.png?raw=true)

![](https://github.com/yaomer/pictures/blob/master/raft-s2.png?raw=true)
