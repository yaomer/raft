raft
===
<table>
<tbody>
<tr>
  <th>领导选举</th>
  <td>✔️</td>
</tr>
<tr>
  <th>日志复制</th>
  <td>✔️</td>
</tr>
<tr>
  <th>日志压缩</th>
  <td>✔️</td>
</tr>
<tr>
  <th>成员变化</th>
  <td>❌</td>
</tr>
<tr>
  <th>状态机接口</th>
  <td>✔️</td>
</tr>
<tr>
  <th>客户端</th>
  <td>✔️</td>
</tr>
</tbody>
</table>

cluster
===
我们以一个简单的运行在`raft`上的[k-v server](https://github.com/yaomer/raft/tree/master/examples/kv)为例

假设集群配置为5节点
```
# cluster nodes
node 127.0.0.1 8000
node 127.0.0.1 8001
node 127.0.0.1 8002
node 127.0.0.1 8003
node 127.0.0.1 8004
```
然后，你可以打开5个终端，在每个终端分别执行
```
$ ./serv 8000
$ ./serv 8001
$ ./serv 8002
$ ./serv 8003
$ ./serv 8004
```
这样就可以将集群跑起来了

你可以动态改变节点的存活情况来观察`raft`的工作原理

+ `raft-node`接收的用户消息格式为`<user>msg\r\n`，响应格式由`上层状态机(k-v)`与客户端约定
    + 目前`k-v`只支持`set`和`get`，所以响应有3种：`+ok\r\n`，`-err\r\n`，`$reply\r\n`
+ 如果`raft-node`发现自己不是`leader`，就会发送`<host>host\r\n`以帮助客户端`重定向到leader`
