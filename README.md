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
或者将被kill掉的节点重新拉起，来观察cluster的工作流程。

但请注意，一旦剩余节点数量小于3，cluster将不能正常工作。

目前你只可以向leader发送如下格式的消息来通信
```
# 前缀<user>和分隔符\r\n必不可少
<user>message\r\n
```

当你通信的对象不是leader时，它只会告诉你`>I'm not a leader<`^o^

虽然很容易告诉客户端当前的leader是谁，但先这样了

程序会打印输出如下的流程信息
```
follower:  my runid is cc75ff98-1994-42bd-9cff-e62bf6b9379d
candidate: start 1th leader election
candidate: ### connect with server 127.0.0.1:8001
candidate: start 2th leader election
leader:    become a new leader
leader:    append new log[0](2, hello<1>)
leader:    log[0](2, hello<1>) is applied to the state machine
leader:    append new log[1](2, hello<2>)
leader:    log[1](2, hello<2>) is applied to the state machine
leader::   ### disconnect with server 127.0.0.1:8001
```

```
follower:  my runid is 98a5bbae-f821-43e2-bf93-82964214030f
follower:  ### connect with server 127.0.0.1:8000
follower:  voted for cc75ff98-1994-42bd-9cff-e62bf6b9379d
follower:  append new log[0](2, hello<1>)
follower:  log[0](2, hello<1>) is applied to the state machine
follower:  append new log[1](2, hello<2>)
follower:  log[1](2, hello<2>) is applied to the state machine
```
