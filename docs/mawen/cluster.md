# 集群

## 一、多master，无slave集群启动

### 1.broker-a master 配置

```broker.conf
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0
deleteWhen=04
fileReservedTime=48
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH

storePathRootDir=D:\develop\github\mawen12\rocketmq\cluster\broker-a-master
```

### 2.Broker-b Master配置

```broker.conf
brokerClusterName=DefaultCluster
brokerName=broker-b
brokerId=0
deleteWhen=04
fileReservedTime=48
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH

storePathRootDir=D:\develop\github\mawen12\rocketmq\cluster\broker-b-master
```

### 3.namesrv 启动命令

```cmd
.\bin\mqnamesrv.cmd 
```

### 4.broker-a 启动命令

```cmd
.\bin\mqbroker.cmd -n 127.0.0.1:9876 -c D:\develop\github\mawen12\rocketmq\cluster\broker-a-master\broker.properties --enable-proxy 
```

### 5.broker-b 启动命令

```cmd
.\bin\mqbroker.cmd -n 127.0.0.1:9876 -c D:\develop\github\mawen12\rocketmq\cluster\broker-b-master\broker.properties --enable-proxy
```

### 6.查看集群信息

```cmd
.\bin\mqadmin.cmd clusterList -n 127.0.0.1:9876
```

### 7.集群内创建主题

在集群内创建主题，一般是把集群内的所有节点都创建相同的主题，确保消息发送时的负载均衡。

```cmd
.\bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -t myTopic -b 127.0.0.1:10911 -r 4 -w 4
```

```cmd
.\bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -t myTopic -b 127.0.0.1:10921 -r 4 -w 4
```

## 二、多master，多slave启动

### 1.broker-a master 配置

```broker.conf
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0
deleteWhen=04
fileReservedTime=48
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH

storePathRootDir=D:\develop\github\mawen12\rocketmq\cluster\broker-a-master
```

### 2.broker-a slave 配置

```broker.conf
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=1
deleteWhen=04
fileReservedTime=48
brokerRole=SLAVE
flushDiskType=ASYNC_FLUSH

storePathRootDir=D:\develop\github\mawen12\rocketmq\cluster\broker-a-slave
```

### 3.broker-b master 配置

```broker.conf
brokerClusterName=DefaultCluster
brokerName=broker-b
brokerId=0
deleteWhen=04
fileReservedTime=48
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH

storePathRootDir=D:\develop\github\mawen12\rocketmq\cluster\broker-a-master
```

### 4.broker-b slave 配置

```broker.conf
brokerClusterName=DefaultCluster
brokerName=broker-b
brokerId=1
deleteWhen=04
fileReservedTime=48
brokerRole=SLAVE
flushDiskType=ASYNC_FLUSH

storePathRootDir=D:\develop\github\mawen12\rocketmq\cluster\broker-a-slave
```

### 5.broker-a master 启动命令

```cmd
.\bin\mqbroker.cmd -n 127.0.0.1:9876 -c D:\develop\github\mawen12\rocketmq\cluster\broker-a-master\broker.properties --enable-proxy 
```

### 6.broker-a slave 启动命令

```cmd
.\bin\mqbroker.cmd -n 127.0.0.1:9876 -c D:\develop\github\mawen12\rocketmq\cluster\broker-a-slave\broker.properties --enable-proxy 
```

### 7.broker-b master 启动命令

```cmd
.\bin\mqbroker.cmd -n 127.0.0.1:9876 -c D:\develop\github\mawen12\rocketmq\cluster\broker-b-master\broker.properties --enable-proxy
```

### 8.broker-b slave 启动命令

```cmd
.\bin\mqbroker.cmd -n 127.0.0.1:9876 -c D:\develop\github\mawen12\rocketmq\cluster\broker-b-slave\broker.properties --enable-proxy
```

### 9.查看集群信息

```cmd
.\bin\mqadmin.cmd clusterList -n 127.0.0.1:9876
```

### 10.集群内创建主题

在集群内创建主题，一般是把集群内的所有节点都创建相同的主题，确保消息发送时的负载均衡。

```cmd
.\bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -t myTopic -b 127.0.0.1:10911 -r 4 -w 4
```

```cmd
.\bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -t myTopic -b 127.0.0.1:10921 -r 4 -w 4
``` 
