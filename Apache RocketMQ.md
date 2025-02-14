# 第一章 RocketMQ 快速了解



## 一、简介

RocketMQ是Alibaba推出的分布式消息中间件。

RocketMQ提供了以下功能：

- 支持Pull模型的消费方式
- 支持TCP协议、JMS规范、OpenMessaging规范
- 支持顺序消息（Ordered Message）
- 支持调度消息（Scheduled Message）
- 支持批量消息（Batched Message）
- 支持广播消息（Broadcast Message）
- 支持消息过滤，基于SQL92的消息属性过滤
- 支持服务端消息重发
- 底层使用高性能、低延迟的文件存储
- 支持基于时间戳（timestamp）和偏移量（offset）的消息追溯
- 支持高可用和容错，基于主从架构



## 二、快速启动



### 1.下载安装包

从Github或官网下载安装包

- https://dist.apache.org/repos/dist/release/rocketmq/5.3.1/rocketmq-all-5.3.1-bin-release.zip
- https://github.com/apache/rocketmq/archive/refs/tags/rocketmq-all-5.3.1.zip



### 2.安装JDK



#### 下载JDK

从Oracle官方下载JDK1.8。

http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html?spm=5238cd80.6a33be36.0.0.39f71e5dUDqr4o



#### 配置JDK

```
vim /etc/profile
```

在末尾添加

```
export JAVA_HOME=/home/mawen/java/jdk1.8.0_171
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:$PATH
```

使环境变量生效

```bash
source /etc/profile
```

添加软连接

```bash
ln -s /home/mawen/java/jdk1.8.0_171/bin/java /usr/bin/java
```

检查

```bash
java -version
```

### 3.解压缩RocketMQ发行包

```bash
unzip rocketmq-all-5.3.1-bin-release.zip
cd rocketmq-all-5.3.1-bin-release
```

### 4.启动 NameServer

#### Linux 单机启动

```bash
sh bin/mqnamesrv
```

### 5.验证NameServer是否启动成功

#### 日志检查

```bash
tail -f ~/logs/rocketmqlogs/namesrv.log
```

### 6.启动 Broker

#### Linux 单机启动

```bash
sh bin/mqbroker -n localhost:9876 --enable-proxy
```

### 7.验证Broker是否启动成功

#### 日志检查

```bash
tail -f ~/logs/rocketmqlogs/proxy.log
```

## 三、集群启动

### 1.Broker-a Master配置

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

# 第二章 RocketMQ Broker







# 第三章 RocketMQ Client



 



































































# RocketMQ 部署架构



RocketMQ 的主要组件：

- Nameserver
- Broker
- Client



## Nameserver



​	Nameserver 集群，topic 的路由注册中心，为客户端根据 Topic 提供路由服务，从而引导客户端向 Broker 发送消息。Nameserver 之间的节点不通信，路由信息在 Nameserver 集群中数据一致性采取的是最终一致性。

​	Nameserver 是在内存中存储 Topic 的路由信息。



## Broker

​	

​	消息存储服务器，分为两种角色：Master 与 Slave。Master 承担读写操作，Slave 作为一个备份。当 Master 存在压力时，Slave 可以承担读服务。所有 Broker，包含 Slave 服务器每隔 30s 会向 Nameserver 发送心跳包，心跳包会包含存在于 Broker 上所有的 topic 的路由信息。

​	Broker 是在磁盘上持久化存储 Topic 路由信息，即 ${ROCKETMQ_HOME}/store/config/topics.json。

​	多副本机制，从 RocketMQ4.5.0 开始引入，即一个复制组（m-s）可以演变为基于 raft 协议的复制组，复制组内部使用 raft 协议保证 broker 节点数据的强一致性，该部署架构在金融行业用的比较多。



### Topic

​	

​	一类消息的集合，Producer 将一类消息发送到一个 topic 中。



### Queue

​	

​	存储消息的实际载体，一个 topic 可以由多个 queue 组成。queue 的数量，决定了最大消费者的个数。

增加 queue 的数量，不会对 RocketMQ 的性能产生影响。

​	RocketMQ 支持对 topic 进行扩容、缩容，其本质就是调整 topic 下 queue 的数量。

​	



## Client



​	消息客户端，包含：Producer 和 Consumer。客户端在同一时间只会连接一台 Nameserver，只有在连接异常时才会尝试连接另外一台。客户端每隔 30s 向 Nameserver 发起 topic 的路由信息查询。



### ConsumerGroup



​	消费组，一个消费单位的群体，consumergroup 在启动时订阅需要消费的 topic，一个 topic 可以被消费组订阅，同样一个 consumergroup 也可以订阅多个 topic，一个 consumergroup 拥有多个 consumer。

​	

# 消息订阅模型



​	RocketMQ 采用发布订阅模式。

​	

## 消费模式



​	广播模式：一个 consumergroup 中的所有消费者每一个都会处理 topic 中的每一条信息，通常用于刷新内存缓存。

​	集群模式：一个 consumergroup 中的所有消费者共同消费一个 topic 中的消息，即分工协作，一个 consumer 消费一部分数据，启动负载均衡。



## 负载均衡算法

​	

​	由于一个 topic 存在多个 queue，而且一个 topic 可以被 consumergroup 订阅，但是 queue 与 consumer 不对等的数量，容易出现负载不均衡的问题。

​	RocketMQ 提供了众多的队列负载算法，常见的有：

- AllocateMessageQueueAveragely 平均分配
- AllocateMessageQueueAveragelyByCircle 轮流平均分配

​	上述的队列负载算法，主要处理 queue 数量大于 consumer 数量的场景。但是对于 queue 数量小于 consumer 数量，就不会应用，这会出现部分的 consumer 无法被分配到消息。



## 消费队列重平衡机制

​	

​	当一个新的 consumer 加入到 consumergroup 中时，此 consumer 应该消费哪个 queue 呢？

​	RocketMQ 会每隔20s去查询当前 topic 的所有 queue，consumer 个数，运用队列负载均衡算法重新分配。



## 消费进度



​	consumer 消费一条消息后需要记录消费的位置，这样在 consumer 重启的时候，继续从上一次消费的位点开始进行处理新的消息。在 RocketMQ 中，消息消费位点的存储时以 consumergroup 为单位的。

​	广播模式：消费进度存储在用户的主目录，默认文件全路径名：${USER_HOME}/.rocketmq_offsets。

​	集群模式：消费进度存储在 broker 端，存储文件路径为：${ROCKETMQ_HOME}/store/config/consumerOffset.json。



## 消费模型



​	RocketMQ 提供了并发消费和顺序消费两种消费方式。

​	并发消费：对一个 queue 中消息，每一个 consumer 内部都会创建一个线程池，对队列中的消息多线程处理，即偏移量大的消息比偏移量小的消息有可能先被消费。

​	顺序消费：一个 consumergroup 中的 consumer 会创建多线程，但是对于同一个 queue，会加锁。



### 消费重试

​	RocketMQ 对并发消费和顺序消费提供了不同的重试机制。

​	并发消费：消费失败默认会重试16次，每一个的间隔时间不一样。

​	顺序消费：消费失败后一直重试，直到消费成功。对于顺序消费的使用过程中，需要区分系统异常和业务异常。并提供告警机制，及时进行人为干预，否则会出现消息积压。



## 消息类型



### 事务消息

​	RocketMQ 提供了事务消息，用于实现最终一致性的场景，而非分布式事务。



### 定时消息

​	RocketMQ 支持将定时消息发送到 broker，但是该消息不会立即被消费，而是要到指定延迟时间后才能被消费。



## 消息过滤



​	RocketMQ 支持 consumer 根据特定条件来对 topic 中的消息进行过滤。

​	RocketMQ 提供了基于 tag 和基于消息属性的过滤。其中基于消息属性的过滤支持 SQL92 表达式。



# 第二章 RocketMQ的安装与启动



## 三、单机安装与启动



### 1.准备工作



### 2.修改初始内存



#### 修改runserver.sh

使用vim命令打开bin/runserver.sh文件。将其中256M修改为1024M。



#### 修改runbroker.sh

使用vim命令打开bin/runbroker.sh文件。将其中256M修改为1024M。



### 3.启动



#### 启动 nameServer

```
nohup sh bin/mqnamesrv &
```



#### 检查日志

```java
tail -f ~/logs/rocketmqlogs/namesrv.log
```



#### 启动 broker

```shell
nohup sh bin/mqbroker -n localhost:9876 &
```



#### 检查日志

```java
tail -f ~/logs/rocketmqlogs/broker.log
```





### 4.发送/接受消息测试



### 4.关闭Server





# 第三章 RocketMQ 工作原理



## 一、消息的生产



### 1.消息生产过程



Producer 可以将消息写入到某 broker 中的某 Queue 中，其经历了如下过程：

- Producer发送消息之前，会先向NameServer发出获取消息Topic的路由信息的请求
- NameServer返回该Topic的==路由表==及==Broker列表==
- Producer根据代码中指定的Queue选择策略，从Queue列表中选出一个队列，用于后续存储消息
- Producer对消息做一些特殊处理，例如：消息本身超过4M，则会对其进行压缩
- Producer向选择出的Queue所在的Broker发出==RPC请求==，将消息发送到选择出的Queue

> 路由表：实际上是一个Map，key为topic名称，value是一个QueueData的实例列表。QueueData并不是一个Queue对应一个QueueData，而是一个Broker中该Topic的所有Queue对应一个QueueData。即只要涉及到该Topic的Broker，一个Broker对应一个QueueData。QueueData中包含brokerName。
> 简单来说，路由表的key为Topic名称，value则为所有涉及该Topic的BrokerName列表。
>
> 
>
> Broker列表：其实际也是一个Map，key为brokerName，value为BrokerData。一个BrokerName相同的Master-Slave小集群对应一个BrokerData实例。BrokerData中包含brokerName以及一个Map。该Map的key为brokerId，value为该broker对应的地址。 brokerId为0表示为该broker为Master，非0表示Slave。



### 2.Queue选择算法



对于无序消息，其Queue选择算法，也称为消息投递算法，常见的有两种：

#### 轮询算法

==默认==选择算法。该算法保证了每个Queue中可以均匀的获取到消息。

> 该算法存在一个问题：由于某些原因，在某些Broker上的Queue可能投递延迟较严重，从而导致Producer的缓存队列中出现较大的==消息积压==。影响消息投递性能。



#### 最小投递延迟算法

该算法会统计每次消息投递的==时间延迟(RT)==，然后根据统计出的结果将消息投递到时间延迟最小的Queue。如果延迟相同，则采用轮询算法投递。该算法可以有效提升消息的投递性能。

> 当Queue之间的延迟过大时，会出现==消息堆积==在局部一两个队列中的情况，即消息在Queue上的分配不均因。在消费端会导致这几个队列的消费者过于繁忙，其他消费者空闲的问题，即消费者的消费不均匀。



## 二、消息的存储



RocketMQ中的消息存储在本地文件系统中，这些相关文件默认在当前用户主目录下的==store==目录中。

- ==abort==：该文件在Broker启动后会自动创建，正常关闭Broker，该文件会自动消失。若在没有启动Broker的情况下，发现这个文件是存在的，则说明之前Broker是非正常关闭。
- ==checkpoint==：存储着commitlog、consumequeue、index文件的最后刷盘时间戳。
- ==commitlog==：存放着commitlog文件，而消息是写在commitlog文件中的。
- ==config==：存放着Broker运行期间的一些配置数据。
- ==consumequeue==：存放着consumequeue文件，队列就存放在这个目录中。
- ==index==：存放着消息索引文件（indexFile）。
- ==lock==：运行期间使用到的全局资源锁。



### 1.commitlog文件

> 说明：在很多资料中，commitlog目录中的文件简单就称为commitlog文件。但在源码中，该文件被命名为mappedFile。



#### 目录与文件

commitlog目录中存放着很多的mappedFile文件，当前Broker中的所有消息都是落盘到这些mappedFile文件中的。mappedFile文件大小为==1G==（小于等于1G），文件名由==20位==十进制数构成，表示当前文件的第一条消息的在所有消息中的起始位置偏移量。

> 第一个文件名一定是20位0构成的。因为第一个文件的第一条消息的偏移量commitlog offset为0。
>
> 当第一个文件放满时，则会自动生成第二个文件继续存放消息，假设第一个文件大小是1073741824（1G），则第二个文件名就是00000000001073741824。
>
> 以此类推，第N个文件名应该是前N-1个文件大小之和。
>
> 一个Broker中所有的mappedFile文件的commitlog offset是连续的。

需要注意的是，一个Broker中仅包含一个commitlog目录，所有的mappedFile文件都是存放在该目录中的。即无论当前Broker中存放着多少Topic的消息，这些消息都是被顺序写入到了mappedFile文件中的。也就是说，这些消息在Broker中存放时并没有按照Topic进行分类存放。

> mappedFile文件是按==顺序读写==的，所有其访问效率很高。
>
> 无论是SSD还是SATA磁盘，通常情况下顺序存取效率都会高于随机存取。



#### 消息单元

 mappedFile文件内容由一个个的==消息单元==构成。每个消息单元中包含消息总长度MsgLen、消息的物理位置physicalOffset、消息体长度BodyLength、消息体内容Body、Topic长度TopicLength、消息主题Topic、消息生产者BronHost、消息发送时间戳BornTimestamp、消息所在的队列QueueId、消息在Queue中存储的偏移量QueueOffset等近20余项消息相关属性。

> 需要注意到，消息单元中包含Queue相关属性的，所以我们就需要十分留意commitlog与Queue之间的关系是什么？
>
> 
>
> 一个mappedFile文件中第m+1个消息单元的commitlog offset偏移量等于L(m+1)=L(m)+MsgLen(m)。



### 2.consumequeue



#### 目录与文件

为了提高效率，会为每个Topic在==~/store/consumequeue==中创建一个==同名目录==。在该Topic目录下，会再为每个该Topic的Queue创建一个目录，目录名为==queueId==。每个目录中存放着若干consumequeue文件，该consumequeue文件是commitlog的==索引==文件，可以根据consumequeue定位到具体的消息。

consumequeue文件名也是由20为数字构成，表示当前文件的第一个索引条目的起始位偏移量。与mappedFile文件名不同的是，其后续文件名是固定的。因为consumequeue文件大小是固定不变的。



#### 索引条目

每个consumequeue文件可以包含30w个索引条目，每条索引条目包含了三个消息重要属性：消息在mappedFile文件中的偏移量CommitLog Offset、消息长度、消息Tag的hashCode值。这三个属性占20字节，索引每个文件的大小是固定的30w * 20 字节。

> 一个consumequeue文件中所有消息的Topic一定是相同的。但每条消息的Tag可能是不同的。



### 3.对文件的读写



#### 消息写入

一条消息进入到Broker后经历了以下几个过程才最终被持久化。

- Broker根据queueId，获取到该消息对应索引条目要在consumequeue目录中的写入偏移量，即QueueOffset
- 将queueId、queueOffset等数据，与消息一起封装为消息单元
- 将消息单元写入到commitLog
- 形成消息索引条目
- 将消息索引条目分发到相应的consumequeue



#### 消息拉取

当Consumer来拉取消息时会经历以下几个步骤：

- Consumer获取到要消费所在Queue的==消费偏移量==offset，计算出要消费消息的消息offset。
- Consumer向Broker发送拉取消息，其中会包含要拉取消息的Queue、消息offset以及消息Tag。
- Broker计算在该consumequeue中的queueOffset。
- 从该queueOffset处开始向后查找第一个指定Tag的索引条目。
- 解析该索引条目的前8个字节，即可定位到该消息在commitLog中的commitLog offset。
- 从对应commitLog offset中读取消息单元，并发送给Consumer。

> 消费offset即消费进度，consumer对某个Queue的消费offset，即消费到了该Queue的第几条消息。
>
> queueOffset = 消息offset * 20字节



#### 性能提升

RocketMQ中，无论时消息本身还是消息索引，都是存储在磁盘上的，其不会影响消息的消费？当然不会。其实RocketMQ的性能在目前的MQ产品中性能是非常高的。因为系统通过一系列相关机制大大提升了性能。

首先，RocketMQ对文件的读写操作是通过mmap零拷贝进行的，将对文件的操作转化为内存地址进行操作，从而极大地提高了文件的读写效率。

其次，consumequeue中的数据是顺序存放的，还引入了PageCache的==预读取机制==，使得对consumequeue文件的读取几乎接近于内存读取，即使在有消息堆积情况下也不会影响性能。

> PageCache机制，页缓存机制。是OS对文件的缓存机制，用于加速对文件的读写操作。一般来说，程序对文件进行顺序读写的速度几乎接近与内存读写速度。主要原因是由于OS使用PageCache机制对读写访问操作进行了性能优化，将一部分的内存用作PageCache。
>
> 
>
> - 写操作：OS会先将数据写入PageCache中，随后会以异步方式由==pdflush==（page dirty flush）内核线程将Cache中的数据刷盘到物理磁盘上。
> - 读操作：若用户要读取数据，其首先会从PageCache中读取，若没有命中，则OS在从物理磁盘上加载该数据到PageCache的同时，也会顺序对其相邻数据块中的数据进行预读取。

RocketMQ中可能会影响性能的是对commitLog文件的读取，因为对commitLog文件来说，读取消息时会产生大量的随机访问，而随机访问会严重影响性能。不过，如果选择合适的系统I/O调度算法，比如设置调度算法为Deadline（采用SSD固态硬盘的话），随机读的性能也会有所提升。



### 4.与Kafka的对比

RocketMQ的很多思想来源于Kafka，其中commitlog与consumequeue就是。

RocketMQ中的commitlog目录与consumequeue的结合就类似于Kafka中的partition分区目录。

mappedFile文件就类似于Kafka中的segment段。

> Kafka中的Topic的消息被分割为一个或多个partition。partition是一个物理概念，对应到系统上就是topic目录下的一个或多个目录。每个partition中包含的文件成为segment，是具体存放消息的文件。
>
> Kafka中消息存放的目录结构：
>
> - topics 目录
>   - partitions 目录
>     - segment 文件
>
> Kafka中没有二级分类标签Tag这个概念。
>
> Kafka中无需索引文件，因为生产者是将消息直接写在partition中的，消费者也是直接从partition中读取数据的。



## 三、indexFile

除了通过通常的指定Topic进行消息消费外，RocketMQ还提供了根据==key==进行消息查询的功能。该查询是通过store目录中的index子目录中的indexFile进行索引实现的快速查询。当然，这个indexFile中的索引数据是在==包含key的消息==被发送到Broker时写入的。如果消息中没有key，则不会写入。



### 1.索引条目结构



每个Broker中会包含一组indexFile，每个indexFile都是以一个==时间戳==命名的（这个indexFile被创建时的时间戳）。每个indexFile文件都是由三部分构成：==indexHeader==、==slots槽位==、==indexes索引数据==。每个indexFile文件中包含500w个slot槽。而每个slot槽又可能会挂载很多的index索引单元。

indexHeader固定==40个字节==，其中存放着如下数据：

- ==beginTimestamp==：该indexFile中第一条消息的存储时间
- ==endTimestamp==：该indexFile中最后一条消息的存储时间
- ==beginPhyoffset==：该indexFile中第一条消息在commitlog中的偏移量commitlog offset
- ==endPhyoffset==：该indexFile中最后一条消息在commitlog中的偏移量commitlog offset
- ==hashSlotCount==：已经填充有index的slot数量（并不是每个slot槽下都挂载有index索引单元，这里统计的是所有挂载了index索引单元的slot槽的数量）
- ==indexCount==：该indexFile中包含的索引单元个数（统计出当前indexFile中所有slot槽下挂载的所有index索引单元的数量之和）



indexFile中最复杂的是Slots与indexes之间的关系。在实际存储时，indexes是在Slots后面的，但为了便于理解，将它们的关系展示为如下形式。



==key的hash值%500w==的结果即为==slot槽位==，然后将该slot值修改为该index索引单元的indexNo，根据这个indexNo可以计算出该index单元在indexFile中的位置。不过，该取模结果的重复率是很高的，为了解决该问题，在每个index索引单元中增加了preIndexNo，用于指定该slot中当前index索引单元的前一个index索引单元。而slot中始终存放的是其下最新的index索引单元的indexNo，这样的话，只需要找到slot就可以找到其最新的index索引单元，而通过这个index索引单元就可以找到其之前的所有index索引单元。

>indexNo是一个在indexFile中的流水号，从0开始依次递增。即在一个indexFile中所有indexNo是依次递增的。
>
>slot槽位的设计类似与HashMap。



index索引单元默认写==20个字节==，其中存放着以下四个属性：

- keyHash：消息中指定的业务key的hash值
- phyOffset：当前key对应的消息在commitlog中的偏移量commitlog offset
- timeDiff：当前key对应消息的存储时间与当前indexFile创建时间的时间差
- preIndexNo：当前slot下当前index索引单元的前一个index索引单元的indexNo



### 2.indexFile的创建

indexFile的文件名为当前文件被创建时的时间戳，这个时间戳有什么用呢？

根据业务key进行查询时，查询条件除了==key==之外，还需要指定一个要查询的==时间戳==，表示要查询不大于该时间戳的最新的消息。即查询指定时间戳之前存储的最新消息。这个时间戳文件名可以简化查询，提高查询效率。

indexFile文件是何时创建的？其创建的条件（时机）如下：

- 当第一条带key的消息发送过来后，系统发现==没有indexFile==，此时会创建第一个indexFile文件
- 当一个indexFile中挂载的index索引单元数量==超过2000w==个（500w槽，平均每个槽位下挂载4个）时，会创建新的indexFile。当带key的消息发送带来后，系统会查找最新的indexFile，并从其indexHeader的最后4字节中读取到indexCount。若indexCount >= 2000w时，会创建新的indexFile。

>由此可以推算出，一个indexFile的最大大小是：40 + 500w * 4 + 2000w * 20 字节



### 3.查询流程

当消费者通过业务key来查询相应的消息时，其需要经过一个相对较复杂的查询流程。不过，在分析查询流程之前，首先要清楚几个定位计算式子：

```
计算指定消息key的slot槽位序号：
slot槽位序号 = hash(key) % 500w
```

```
计算槽位序号为n的slot在indexFile中的起始位置
slot(n)位置 = 40 + (n - 1) * 4
```

```
计算indexNo为m的index在indexFile中的位置
index(m)位置 = 40 + 500w * 4 + (m -1) * 20
```

> 40 为indexFile中indexHeader的字节数
>
> 500w * 4 是所有slots所占的字节数



具体查询流程为：

1. 根据传入时间找到相应的indexFile
2. 计算传入时间与找到的indexFile文件名间的差值diff
3. 计算出业务key的hash值
4. 计算出slot槽位序号
5. 根据slot槽位好计算出该slot在indexFile中的位置
6. 找到slot后读取slot值，即当前slot中最新的index索引单元的indexNo
7. 根据indexNo计算出该index单元在indexFile的位置
8. 根据计算出的时间差 - 当前indexFile单元中的timeDiff
9. 如果结果 < 0，则读取该index单元的preIndexNo，重新计算比较；否则读取该index单元的physicalOffset
10. 根据physicalOffset，定位commitlog中消息的位置



## 四、消息的消费

消费者从Broker中==获取消息==的方式有两种：pull拉取方式和push推动方式。消费者组对于==消息消费==的模式又分为两种：集群消费Clustering和广播消费Broadcasting。



### 1.获取消费类型



#### 拉取式消费

Consumer主动从Broker中==拉取==消息，主动权由Consumer控制。一旦获取了批量消息，就会启动消费过程。

不过，该方式的实时性较弱，即Broker中有了新的消息时消费者并不能及时发现并消费。

> 由于拉取时间间隔是由用户指定的，所以在设置间隔时需要注意平稳：间隔太短，空请求比例会增加；间隔太长，消息的实时性太差。



#### 推送式消费

该模式下Broker收到数据后会==主动推送==到Consumer。该消费模式一般==实时性较高==。

该消费类型是典型的`发布-订阅`模式，即Consumer向其关联的Queue注册了监听器，一旦发现有新的消息到来就会触发回调的执行，回调方法是Consumer去Queue中拉取消息。而这些都是基于Consumer与Broker间的长连接的。==长连接==的维护是需要消耗系统资源的。



#### 对比

- pull：需要应用去实现对关联Queue的遍历，实时性差；但便于应用控制消息的拉取。
- push：封装了对关联Queue的遍历，实时性强，但会占用较多的系统资源。



### 2.消费模式



#### 广播消费

广播消费模式下，相同Consumer Group中的每个Consumer实例都接收同一个Topic的全量消息。即每条消息都会被发送到Consumer Group中的==每个==Consumer。



#### 集群消费

集群消费模式下，相同Consumer Group的每个Consumer实例==平均分摊==同一个Topic的消息。即每条消息只会被发送到Consumer Group中的==某个==Consumer。



#### 消息进度保存

- 广播模式：消费进度保存在==Consumer==端。因为广播模式下Consumer Group中每个consumer都会消费所有消息，但它们的消费进度是不同的。所以consumer各自保存各自的消费进度。
- 集群模式：消费进度保存在==Broker==中。consumer group中的所有consumer共同消费同一个Topic中的消息，同一条消息只会被消费一次。消费进度参与到了消费的负载均衡中，故消费进度是需要共享的。

> 消费进度保存在~/store/config/consumerOffset.json文件中。 



### 3.Rebalance机制



Rebalance机制讨论的前提是：==集群消费==。



#### 什么是Rebalance

Rebalance 即再均衡，指的是将一个Topic下的多个Queue在同一个Consumer Group中的多个Consumer之间进行重新再分配的过程。

Rebalance机制的本意是为了提升消息的==并行消费==能力。例如：一个Topic下5个队列，在只有一个消费者的情况下，这个消费者将负责消费这5个队列的消息。如果此时我们增加一个消费者，那么就可以给其中一个消费者分配2个队列，给另一个分配3个队列，从而提升消息的并行消费能力。



#### Rebalance限制

由于一个队列最多分配给一个消费者，因此当某个消费组下的消费者实例数量大于队列的数量时，对于的消费者实例将分配不到任何队列。



#### Rebalance危害

Rebalance在提升消费能力ide同时，也带来一些问题：

==消费暂停==：在只有一个Consumer时，其负责消费所有队列；在新增了一个Consumer后会触发Rebalance的发生。此时原Consumer就需要==暂停==所有队列的消费，等待这些队列分配给新的Consumer后，这些暂停消费的队列才能继续被消费。

==消费重复==：Consumer在消费新分配给自己的队列时，必须接着之前Consumer提交的消费进度的offset继续消费。然而默认情况下，offset是==异步提交==的。这个异步性导致提交到Broker的offset和Consumer实际消费的消息并不一致。这个不一致的差值就是可能会导致消息重复消费。

> 同步提交：consumer提交了其消费完毕的一批消息的offset给broker后，需要等待broker的成功ACK。当收到ACK后，consumer才会继续获取并消费下一批消息。在等待ACK期间，consumer是阻塞的。
>
> 异步提交：consumer提交了其消费完毕的一批消息的offset给broker后，不需要等待broker的成功ACK。consumer可以直接获取并消费下一批消息。
>
> 对于一次性读取消息的数量，需要根据具体业务场景，选择一个相对均衡的是很有必要的。因为数量过大，系统性能提升了，但产生重复消费的消息数量可能会增加。数量过小，系统性能会下降，但被重复消费的消息数量可能会减少。

==消息突刺==：由于Rebalance可能导致重复消费，如果需要重复消费的消息过多，或者因为Rebalance暂停时间过长导致积压了部分消息。那么有可能导致在Rebalance结束之后瞬间需要消费很多消息。



#### Rebalance产生的原因？

导致Rebalance产生的原因，无非就两个：消费者所订阅的Queue数量发生变化，或消费者组中消费者的数量发生变化。

> Queue数量发生变化的场景：
>
> - Broker扩容或缩容
> - Broker升级运维
> - Broker和NameServer间的网络异常
> - Queue扩容或缩容
>
> 消费者数量发生变化的场景：
>
> - Consumer Group扩容或缩容
> - Consumer升级运维
> - Consumer与NameServer间网络异常



#### Rebalance过程

在Broker中维护着多个==Map集合==，这些集合中==动态==存放着当前Topic中Queue的信息、Consumer Group中Consumer实例的信息。一旦发现消费者所订阅的Queue数量发生变化，或消费者组中消费者的数量发生变化，立即向Consumer Group中的每个实例发出Rebalance通知。

> TopicConfigManager：key是topic名称，value是topicConfig。TopicConfig中维护着该Topic中所有Queue的数据。
>
> ConsumerManager：key是Consumer Group Id，value是ConsumerGroupInfo。ConsumerGroupInfo中维护着该Group中所有Consumer实例数据。
>
> ConsumerOffsetManager：key是Topic与订阅该Topic的Group的组合，value是一个内层Map，内层Map的key为QueueId，内层Map的value为该Queue的消费进度offset。

Consumer实例在接收到通知到后，采用==Queue分配算法==自己获取到相应的Queue，即由Consumer实例==自主==进行Rebalance。



#### 与kafka对比

在kafka中，一旦发现出现了Rebalance条件，Broker会调用Group Coordinator来完成Rebalance。

Coordinator是Broker中的一个进程，Coordinator会在Consumer Group中选出一个Group Leader。由这个Leader根据自己本身组情况完成Partition分区的再分配。这个再分配结果会上报给Coordinator，并由Coordinator同步给Group中的所有Consumer实例。

Kafka中的Rebalance是由Consumer Leader完成的，而RocketMQ中的Rebalance是由每个Consumer自身完成的，不存在Leader。



### 4.Queue分配算法

一个Topic中的Queue只能由Consumer Group中的一个Consumer进行消费，而一个Consumer可以同时消费多个Queue中的消息。那么Queue与Consumer间的配对关系是如何确定的？即Queue要分配给哪个Consumer去消费，也是有算法策略的。常见的有四种策略，这些策略是通过在==创建Consumer==时的构造器传进去的。



#### 平均分配策略

该算法是要根据avg=QueueCount/ConsumerCount的计算结果进行分配的。如果能够整除，则按照顺序将avg个Queue逐个分配Consumer；如果不能整除，则将多余出的Queue按照Consumer顺序逐个分配。

> 该算法即先计算好每个Consumer应该分得几个Queue，然后依次将这些数量的Queue逐个分配Consumer。
>
> 算法简单，分配效率高。



#### 环形平均策略

环形平均算法是指，根据消费者的顺序，依次在由queue队列组成的环形图中逐个分配。

> 该算法不用事先计算每个Consumer需要分配几个Queue，直接一个一个分即可。
>
> 算法简单，分配效率高。



#### 一致性hash策略

该算法会将==consumer的hash值==作为Node节点存放到hash环上，然后将==queue的hash值==也放到hash环上。通过==顺时针==方向，距离queue最近的那个consumer就是该queue要分配的consumer。

>该算法存在的问题：分配不均。



#### 同机房策略

该算法会根据queue的部署机房位置和consumer的位置，过滤出当前consumer相同机房的queue。然后按照平均分配策略或环形平均策略对同机房queue进行分配。如果没有同机房queue，则按照平均分配策略或环形平均策略对所有queue进行分配。



#### 分配算法对比

一致性hash算法存在的问题：

​	两种平均分配策略的分配效率较高，一致性hash策略的较低。因为一致性hash算法较复杂。另外，一致性hash策略分配的结果有很大可能存在不平均的情况。

一致性hash算法存在的意义：

​	其可以==有效减少==由于==消费者组==扩容或缩容所带来的大量的==Rebalance==。

一致性hash算法应用场景：

​	Consumer数量变化较频繁的场景。



### 5.至少一次原则

RocketMQ有一个原则：每条消息必须要被==成功==消费一次。

那么什么是成功消息呢？Consumer在消费完消息后会向其==消息进度记录器==提交其消费消息的offset，offset被成功记录到记录器中，那么这条消息就被成功消费了。

> 什么是消费进度记录器？
>
> ​	对于广播消费模式来说，Consumer本身就是消费进度记录器。
>
> ​	对于集群消费模式来说，Broker是消费进度记录器。



## 五、订阅关系的一致性

订阅关系的一致性指的是，==同一个消费者组==（Group ID相同）下所有Consumer实例所订阅的Topic与Tag及==对消息的处理逻辑==必须==完全一致==。否则，消息消费的逻辑就会==混乱==，甚至导致消息==丢失==。



### 1.正确订阅关系

多个消费者组订阅了多个Topic，并且每个消费者组里的多个消费者实例的订阅关系保持了一致。



### 2.错误订阅关系

一个消费者组订阅了多个Topic，但是该消费组里的多个Consumer实例的订阅关系并没有保持一致。



#### 订阅不同Topic

该例中的错误在于，同一个消费组组中的两个Consumer实例订阅了==不同的Topic==。

Consumer实例1-1：订阅了topic为jodie_test_A，tag为所有的消息

```java
Properties properties = new Properties();
properties.put(PropertyKeyConst.GROUP_ID, "GID_jodie_test_1");
Consumer consumer = ONSFactory.createConsumer(properties);
consumer.subscribe("jodie_test_A", "*", new MessageListener() {
   public Action consume(Message message, ConsumeContext context) {
       System.out.println(message.getMsgID());
       return Action.CommitMessage;
   } 
});
```



Consumer实例1-2：订阅了topic为jodie_test_B，tag为所有的消息

```java
Properties properties = new Properties();
properties.put(PropertyKeyConst.GROUP_ID, "GID_jodie_test_1");
Consumer consumer = ONSFactory.createConsumer(properties);
consumer.subscribe("jodie_test_B", "*", new MessageListener() {
   public Action consume(Message message, ConsumeContext context) {
       System.out.println(message.getMsgID());
       return Action.CommitMessage;
   } 
});
```



#### 订阅了不同Tag

该例中的错误在于，同一个消费者组中的两个Consumer订阅了相同Topic的==不同Tag==。

Consumer实例2-1：订阅了topic为jodie_test_A，tag为TagA的消息

```java
Properties properties = new Properties();
properties.put(PropertyKeyConst.GROUP_ID, "GID_jodie_test_1");
Consumer consumer = ONSFactory.createConsumer(properties);
consumer.subscribe("jodie_test_A", "TagA", new MessageListener() {
   public Action consume(Message message, ConsumeContext context) {
       System.out.println(message.getMsgID());
       return Action.CommitMessage;
   } 
});
```



Consumer实例2-2：订阅了topic为jodie_test_A，tag为所有的消息

```java
Properties properties = new Properties();
properties.put(PropertyKeyConst.GROUP_ID, "GID_jodie_test_1");
Consumer consumer = ONSFactory.createConsumer(properties);
consumer.subscribe("jodie_test_A", "*", new MessageListener() {
   public Action consume(Message message, ConsumeContext context) {
       System.out.println(message.getMsgID());
       return Action.CommitMessage;
   } 
});
```



#### 订阅了不同数量的Topic

该例中的错误在于，同一个消费者组中的两个Consumer订阅了==不同数量的Topic==。

Consumer实例3-1：订阅了两个Topic

```java
Properties properties = new Properties();
properties.put(PropertyKeyConst.GROUP_ID, "GID_jodie_test_1");
Consumer consumer = ONSFactory.createConsumer(properties);
consumer.subscribe("jodie_test_A", "*", new MessageListener() {
   public Action consume(Message message, ConsumeContext context) {
       System.out.println(message.getMsgID());
       return Action.CommitMessage;
   } 
});
consumer.subscribe("jodie_test_B", "*", new MessageListener() {
   public Action consume(Message message, ConsumeContext context) {
       System.out.println(message.getMsgID());
       return Action.CommitMessage;
   } 
});
```



Consumer实例3-2：订阅了一个Topic

```java
Properties properties = new Properties();
properties.put(PropertyKeyConst.GROUP_ID, "GID_jodie_test_1");
Consumer consumer = ONSFactory.createConsumer(properties);
consumer.subscribe("jodie_test_A", "*", new MessageListener() {
   public Action consume(Message message, ConsumeContext context) {
       System.out.println(message.getMsgID());
       return Action.CommitMessage;
   } 
});
```



## 六、offset管理

消费进度offset是用来记录每个Queue的不同消费组的消费进度的。根据消费进度记录器的不同，可以分为两种模式：本地模式和远程模式。

> 这里的offset指的是Consumer的消费进度offset。



### 1.offset本地管理模式

当消费模式为广播消息时，offset使用本地模式存储。因为每条消息都会被所有的消费者消费，每个消费者管理自己的消费进度，各个消费者之间不存在消费进度的交集。

Consumer在广播消费模式下offset相关的数据以json的形式持久化到Consumer本地磁盘文件中，默认文件路径为`~/.rocketmq_offsets/${clientId}/group/Offsets.json`。其中${clientId}为当前消费者id，默认为ip@DEFAULT；group为消费者组名称。



### 2.offset远程管理模式

当消费模式为集群消费时，offset使用远程模式管理。因为所有Consumer实例对消息采用的时均衡消费，所有Consumer共享Queue的消费进度。

Consumer在集群消费模式下offset相关数据以json的形式持久化到Broker磁盘文件中，文件路径为`~/store/config/consumerOffset.json`。

Broker启动时会加载这个文件，并写入到一个双层Map（ConsumerOffsetManager）。外层Map的key为topic@group，value为内层Map。内层Map的key为queueId，value为offset。当发生Rebalance时，新的Consumer会从该Map中获取到相应的数据来继续消费。

集群模式下offset采用远程管理模式，主要是为了==保证Rebalance机制==。



### 3.offset用途

这里有个问题：消费者是如何知道其要消费哪个消息的，即消费者是如何知道要消费消息在Queue中的偏移量offset的？其实在消费者要消费的第一条消息是通过consumer.setConsumerFromWhere()方法指定起始位置的。

在消费者启动后，其要消费的第一条消息的起始位置常用的有三种，这三种位置可以通过枚举类型常量设置。这个枚举类型为==ConsumeFromWhere==。

```java
enum ConsumerFromWhere {
    // 从Queue的当前最后一条消息开始消费
    CONSUME_FROM_LAST_OFFSET,
    
    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    @Deprecated
    CONSUME_FROM_MIN_OFFSET,
    @Deprecated
    CONSUME_FROM_MAX_OFFSET,
    // 从Queue的第一条消息开始消费
    CONSUME_FROM_FIRST_OFFSET,
    // 从指定的具体时间戳位置的消息开始消费，这个具体时间戳是通过另外一个语句指定的，consumer.setConsumeTimestamp()
    CONSUME_FROM_TIMESTAMP,
}
```

当消费完一批消息后，Consumer会提交其消费进度offset给Broker，Broker在收到消费进度后会将其更新到那个双层Map及consumerOffset.json文件中。然后向该Consumer进行ACK，而ACK内容中包含三项数据：当前消费队列的最小offset(minOffset)、最大offset(maxOffset)、及下次消费的起始offset(nextBeginOffset)。



### 4.重试队列

当RocketMQ对消息的消费出现异常的时候，会将发生异常的消息的offset提交到Broker中的重试队列。系统在发生消息消费异常的时候会为当前的Topic@Group创建一个重试队列，该队列以%RETRY%开头，到达重试时间后进行消费重试。



### 5.offset的同步提交与异步提交

集群消费模式下，Consumer消费完消息后会向Broker提交消息进度offset，其提交方式分为两种：

- ==同步提交==：消费者在消费完一批消息后会向Broker提交这些消息的offset，然后等待broker的成功响应。若在等待超时之前收到了成功响应，则继续读取下一批消息进行消费==（从ACK中获取nextBeginOffset）==。若没有收到响应，则会重新提交，直到获取到响应。而在这个等待过程中，消费者是==阻塞==的，其==严重影响了消费者的吞吐量==。
- ==异步提交==：消息者在消费完一批消息后会向Broker提交这些消息的offset，但无需等待Broker的成功响应，可以继续读取并消费下一批消息，这种方式增加了消费者的吞吐量。但需要注意，broker在收到提交的offset后，还是会向消费者进行响应的。可能还没有收到ACK，此时Consumer会==从Broker直接获取nextBeginOffset==。



## 七、消息幂等



### 1.什么是消息幂等

当出现消费者对某条消息重复消费的情况时，==重复消费的结果与消费一次的结果是相同的==，并且多次消费并未对业务系统产生任何负面影响， 那么这个消费过程就是幂等的。

> 幂等：若某操作执行多次与执行一次对系统产生的影响是相同的，则称该操作是幂等的。

在互联网应用中，尤其在网络不稳定的情况下，消息很有可能会出现重复发送或重复消费。如果重复的消息可能会影响业务处理，那么就应该对消息做幂等处理。



### 2.消息重复的场景分析

什么情况下可能会出现消息被重复消费？最常见的有以下三种情况：



#### 发送时消息重复

当一条消息已经被成功发送到Broker并完成持久化，此时出现了网络闪断，从而导致Broker对Producer应答失败。如果此时Producer意识到==消息发送失败并尝试再次发送消息==，此时Broker中就有可能出现两条内容相同并且==Message ID也相同==的消息，那么后续Consumer就一定会消费两次消息。



#### 消费时消息重复

消息已投递到Consumer并完成业务处理，当Consumer给Broker反馈应答时网络闪断，Broker没有接收到消费成功响应。为了保证消息==至少被消费一次==的原则，Broker将在网络恢复后再次投递之前已被处理过的消息。此时消息就会收到与之前处理过的内容相同，Message ID也相同的消息。



#### Rebalance 时消息重复

当Consumer Group中的Consumer数量发生变化时，或者其订阅的Topic的Queue数量发生变化时，会触发Rebalance，此时Consumer可能会收到曾经被消费过的消息。



### 3.通用解决方案

#### 两要素

幂等解决方案的设计中涉及到两项要素：==幂等令牌==与==唯一性处理==。只要充分利用好这两要素，就可以设计出好的幂等解决方案。

- 幂等令牌：是生产者和消费者两者中的既定协议，通常指具备唯一业务标识的字符串。例如订单号、流水号。一般由Producer随着消息一同发送来的。
- 唯一性处理：服务端通过采用一定的算法策略，保证同一个业务逻辑不会被重复执行成功多次。



#### 解决方案

对于常见的系统，幂等性操作的通用性解决方案是：

1. 首先通过缓存去重。在缓存中如果已经存在了某幂等令牌，则说明本次操作是重复性操作；若缓存没有命中，则进入下一步。
2. 在唯一性处理之前，先在数据库中查询幂等操作令牌作为索引的数据是否存在。若存在，则说明本次操作为重复性操作；若不存在，则进入下一步。
3. 在同一事务中完成三项操作：唯一性处理后，将幂等令牌写入到缓存，并将幂等令牌作为唯一索引的数据写入到DB中。

> 第1步已经判断过是否重复性操作了，为什么第2步还要再次判断？能够进入第2步，说明已经不是重复操作了，第2次判断是否重复？
>
> 当然不重复。一般缓存中的数据是具有有效期的。缓存中数据的有效期一旦==过期==，就会发生==缓存穿透==，使请求直接就到达了DBMS。



#### 解决方案举例

以支付场景为例：

1. 当支付请求达到后，首先在Redis缓存中去获取key为==支付流水号==的缓存value。若value不空，则说明本次支付是重复操作，业务系统直接返回调用侧重复支付标识；若value为空，则进入下一步操作
2. 到DBMS中根据==支付流水号==查询是否存在相应实例。若存在，业务系统直接返回调用侧重复支付标识；若不存在，则说明本次操作是首次操作，进入下一步完成唯一性处理
3. 在分布式事务中完成三项操作：
   - 完成支付任务
   - 将==支付流水号==作为key，任意字符串作为value，通过set(key, value, expireTime)将数据写入Redis缓存
   - 将当前==支付流水号==作为主键，与其它相关数据共同写入到DBMS



### 4.消费幂等的实现

消费幂等的解决方案很简单：为消息指定==不会重复的唯一标识==。因为Message ID有可能出现重复的情况，所以真正安全的幂等处理，==不建议以Message ID作为处理依据==。最好的方式是以==业务唯一标识==作为幂等处理的关键依据，而业务的唯一表示可以通过==消息key==设置。

以支付场景为例，可以将消息的Key设置为订单号，作为幂等处理的依据。具体代码示例如下：

```java
Message message = new Message();
message.setKey("ORDERID_100");
SendResult result = producer.send(message);
```



消费者收到消息是可以根据消息的Key即订单号来实现消费幂等：

```java
consumer.registerMessageListener(new MessageListenerConcurrently() {
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for(MessageExt msg:msgs) {
	        String key = msg.getKeys();
            // 根据业务唯一标识Key做幂等处理
            // ...
        }
        return ConsumeConcurrentlyStatus.CONSUME.SUCCESS;
    }
});
```

>  RocketMQ能够保证消息不丢失，但不能保证消息不重复。



## 八、消息堆积与消费延迟



### 1.概念

消息处理过程中，如果Consumer的==消费速度==跟不上Producer的==生产速度==，MQ中未处理的消息会越来越多（进的多出得少），这部分消息就被成为==堆积消息==。消息出现堆积进而会造成消息的==消费延迟==。以下场景需要重点关注消息堆积和消费延迟问题：

- 业务系统上下游==能力不匹配==造成的持续堆积，且无法自行恢复。
- 业务系统对消息的==消费实时性要求较高==，即使是暂时的堆积造成的消费延迟也无法接受。



### 2.产生原因分析

Consumer使用长轮询Pull模式消费消息时，分为以下两个阶段：

#### 拉取消息

Consumer通过==长轮询Pull模式批量拉取==的方式从服务端获取消息，将拉取到的消息缓存到本地缓冲队列中。对于拉取式消费，在内网环境下会有很高的吞吐量，所以这一阶段一般不会成为消息堆积的瓶颈。

>一个单分区单线程的低规格主机Consumer，4C8G，其可达到几万的TPS。
>
>如果是多分区多线程，则可以轻松达到几十万的TPS。
>
>TPS：从长轮询拉取开始，到获得对应的消息的数量与时长的比值。



#### 消息消费

Consumer将本地缓存的消息提交到消费线程中，使用业务消费逻辑对消费进行处理，处理完毕后获取到一个结果。这是真正的消息消费过程。此时Consumer的消费能力就完全依赖于==消息的消费耗时==和==消费并发度==了。如果由于业务处理逻辑复杂等原因，导致处理单条消息的耗时较长，则整体的消息吞吐量肯定不会高，此时就会导致Consumer本地缓冲队列达到上限，停止从服务段拉取消息。

> 消息的消费耗时：单条消息的消费时长
>
> 消费并发度：同时消费多少条消息



#### 结论

消息堆积的主要瓶颈在于客户端的消费能力，而消费能力由==消费耗时==和==消费并发度==决定。主要消费耗时的优先级要高于消费并发度。即在保证了消费耗时的合理性前提下，再考虑消费并发度问题。



### 3.消费耗时

影响消息处理时长的==代码逻辑==，可能主要产生于两种类型的代码：==CPU内部计算型代码==和==外部I/O操作型代码==。

通常情况下代码中如果没有复杂的递归和循环的话，内部计算耗时相对外部I/O操作来说几乎可以忽略。所以==外部I/O型代码一般是影响消息处理时长的主要症结所在==。

> 外部I/O操作型代码：
>
> - 读写外部数据库，例如对远程MySQL的访问
> - 读写外部缓存，例如对远程Redis的访问
> - 下游系统调用，例如：Dubbo的RPC远程调用，Spring Cloud的对下游系统的Http接口调用

> 关于下游系统调用逻辑需要进行提前梳理，掌握每个调用操作预期的耗时，这样做是为了能够判断消费逻辑中I/O操作的耗时是否合理。通常消息堆积是由于下游系统出现了==服务异常==或==达到了DBMS的容量限制==，导致消费耗时增加。
>
> - 服务异常，并不仅仅是系统中出现的类似500这样的代码错误，而可能是更加隐蔽的问题，例如：网络带宽问题。
> - 达到了DBMS容量限制，其也会引发消息的消费耗时增加。



### 4.消费并发度

一般情况下，消费者端的消费并发度由单节点线程数和节点数量共同决定的，其值为==单节点线程数*节点数==。不过，通常需要有优先调整单节点的线程数，若单机硬件资源达到了上限，则需要通过==横向扩展==来提高消费并发度。

> 单节点线程数，即单个Consumer所包含的线程数量
>
> 节点数量：即Consumer Group所包含的Consumer数量
>
> 对于普通消息、延时消息及事务消息，并发计算都是==单节点线程数*节点数==。但对于顺序消息，则是==Topic的Queue分区数量==。
>
> 1. 全局顺序消息：该类型消息的Topic只有一个Queue分区，可以保证该Topic的所有消息可以被顺序消费。为了保证这个全局顺序性，ConsumerGroup中在同一时刻只能有一个Consumer的一个线程进行消费，其并发度为1。
> 2. 分区顺序消息：该类型消息的Topic有多个Queue分区。其仅可以保证该Topic的每个Queue分区中的消息被顺序消费，不能保证整个Topic中消息的顺序消费。为了保证这个分区顺序性，每个Queue分区中的消息在Consumer Group中的同一时刻只能有一个Consumer的一个线程进行消费。即在同一时刻最多会出现多个Queue分区有多个Consumer的多个线程并行消费。所以其并发度为Topic的分区数。



### 5.单机线程数计算

对于一台主机中线程池中线程数的设置需要谨慎，不能盲目直接调大线程数，设置过大的线程数反而会带来大量的线程切换的开销。==理想环境==下单节点的最优线程数计算模型为：==C * (T1 + T2) / T1==。

- C：CPU内核数
- T1：CPU内部逻辑计算耗时
- T2：外部I/O操作耗时

> C * (T1 + T2)/T1 = C * T1/T1 + C * T2/T1 = C + C * T2/T1
>
> 注意，该计算出的数值是理想状态下的理论数据，在生产环境下，不建议直接使用，而是根据当前环境，先设置一个比该值小的数据然后观察其压测效果，然后再根据效果逐步调大，直至找到在该环境中性能最佳时的值。



### 6.如何避免

为了避免在业务使用时出现非预期的消息堆积和消费延迟问题，需要在前期设计阶段对整个业务逻辑进行完善的排查和梳理。其中最重要的就是梳理消息的==消费耗时==和消息消费的==并发度==。



#### 梳理消息的消费耗时

通过压测获取消息的消费耗时，并对耗时较高的操作进行代码逻辑进行分析。梳理消息的消费耗时需要关注以下信息：

- 消费逻辑的==计算复杂度==是否过高，代码是否存在无限循环和递归等缺陷
- 消费逻辑中的==I/O操作==（如：外部调用、读写存储等）是否是必须的，能否用本地缓存等方案规避。
- 消费逻辑中的复杂耗时的操作是否可以做==异步化==处理，如果可以，是否会造成逻辑混乱。



#### 设置消费并发度

对于消费并发度的计算，可以通过以下两步实施：

- 逐步调大单个Consumer节点的线程数，并观测节点的系统指标，得到单个节点最优的消费线程数和消息吞吐量。
- 根据上下游链路的流量峰值计算出需要设置的节点数。

> 节点数=流量峰值 / 单个节点消息吞吐量



## 九、消息的清理

消息被消费过后会被清理掉吗？不会的。

消息是顺序存储在commitlog文件的，且消息大小不定长，所以消息的清理是不可能以消息为单位进行清理的，而是以==commitlog==文件为单位进行清理的。否则会急剧下降清理效率，并实现逻辑复杂。

commitlog文件存在一个==过期时间==，默认为72小时，即三天。除了用户手动清理外，在以下情况也会被自动清理，无论文件中的消息是否被消费过：

- 文件过期，且到达==清理时间点==（默认为凌晨4点）后，自动清理过期文件

  > 在配置文件中通过 deleteWhen 来设置

- 文件过期，且磁盘空间占用率已达==过期清理警戒线==（默认75%）后，无论是否达到清理时间点，都会自动清理过期文件

- 磁盘占用率达到==清理警戒线==（默认85%）后，开始按照设定好的规则清理文件，无论是否过期。默认从==最老==的文件开始清理。

- 磁盘占用率达到==系统危险警戒线==（默认90%）后，Broke将==拒绝==消息写入

> 单个commitlog文件最大大小是1G。
>
> 需要注意以下几点：
>
> 1. 对于RocketMQ系统来说，删除一个1G大小的文件，是一个压力巨大的I/O操作，在删除过程中，系统性能会骤然下降。所以其默认清理时间点为凌晨4点，访问量最小的时候。正因如此，我们要保证磁盘空间的空闲率，不要使系统出现在其他时间点删除commitlog文件的情况。
> 2. 官方建议RocketMQ所在的Linux文件系统采用ext4。因为对于文件删除操作，ext4要比ext3性能更好。
> 3. 消息堆积和消息延迟，有可能导致消息无法写入的情况。





# 第四章 RocketMQ应用



## 一、普通消息



### 1.消息发送分类

Producer对于消息的发送方式也有多种选择，不同的方式会产生不同的系统效果。



#### 同步发送消息

同步发送消息指的是，Producer发出一条消息后，会在收到MQ返回的ACK之后才发下一条消息。该方式的消息==可靠性最高==，但是消息发送==效率太低==。



#### 异步发送消息

异步发送消息是指，Producer发出消息后无需等待MQ返回ACK，直接发送下一条消息。该方式的消息==可靠性可以得到保障==，消息发送==效率也可以==。



#### 单向发送消息

单向发送消息是指，Producer仅负责发送消息，不等待、不处理MQ的ACK。该发送方式时MQ也不返回ACK。该方式的消息发送==效率最高==，但消息==可靠性较差==。



### 2.代码示例



#### 创建工程

创建一个Maven的Java工程rocketmq-test。



#### 导入依赖

导入rocketmq的client依赖。依赖的版本需要和RocketMQ的版本保持一致。

```xml
<dependencies>
	<dependency>
    	<groupId>org.apache.rocketmq</groupId>
        <artifactId>rocketmq-client</artifactId>
        <version>4.8.0</version>
    </dependency>
</dependencies>
```



#### 定义同步消息发送生产者

```java
// 创建一个producer，参数为Producer Group名称
DefaultMQProducer producer = new DefaultMQProucer("pg");
// 指定nameServer地址
producer.setNamesrvAddr("localhost:9876");
// 设置当发送失败时重试发送的次数，默认为2次
producer.setRetryTimesWhenSendFailed(3);
// 设置发送超时时限为5s，默认为3s
producer.setSendMsgTimeout(5000);
// 开启生产者
producer.start();

// 生产并发送100条消息
for (int i = 0; i < 100; i++) {
    // 构造消息体
	byte[] body = ("Hi," + i).getBytes();
    // 构造消息，设置Topic、Tag和消息体
    Message msg = new Message("someTopic", "someTag", body);
    // 为消息指定key
    msg.setKeys("key-" + i);
    // 发送消息并获取Broker响应
    SendResult sendResult = producer.send(msg);
    System.out.println(sendResult);
}
producer.shutdown();
```

```java
// 消息发送状态
public enum SendStatus {
    SEND_OK,				// 发送成功
    FLUSH_DISK_TIMEOUT, 	// 刷盘超时，当Broker设置的刷盘策略为同步刷盘时，才有可能出现这种异常状态。异步刷盘不会出现
    FLUSH_SLAVE_TIMEOUT,	// Slave同步超时。当Broker集群设置的Master-Slave的复制方式为同步复制时，才可能出现这种异常状态，异步复制不会去检查，不会出现
    SLAVE_NOT_AVAILABLE, 	// 没有可用的Slave。当Broker集群设置为Master-Slave的复制方式为同步复制时，才可能出现这种异常，异步复制不会去检查，不会出现
}
```



#### 订阅异步消息发送生产者

```java
// 创建一个producer，参数为Producer Group名称
DefaultMQProducer producer = new DefaultMQProucer("pg");
// 指定nameServer地址
producer.setNamesrvAddr("localhost:9876");
// 设置发送超时时限为5s，默认为3s
producer.setSendMsgTimeout(5000);
// 指定异步发送失败以后不进行重试发送
producer.setRetryTimesWhenSendAsyncFailed(0);
// 指定新的创建Topic的queue数量为2，默认为4
producer.setDefaultTopicQueueNums(2);

// 开启生产者
producer.start();

// 生产并发送100条消息
for (int i = 0; i < 100; i++) {
    // 构造消息体
	byte[] body = ("Hi," + i).getBytes();
    // 构造消息，设置Topic、Tag和消息体
    Message msg = new Message("myTopic", "myTag", body);
    // 为消息指定key
    msg.setKeys("key-" + i);
    // 异步发送，指定回调
    producer.send(msg, new SendCallback() {
        // 当producer接收到MQ发送来的ACK后就会触发该回调方法的执行
        @Override
        public void onSuccess(SendResult sendResult){
        	System.out.println(sendResult);
        }
        
        @Override
        public void onException(Throwable e) {
			e.printStackTrace();
        }
    });
}
// sleep 一会儿
// 由于采用的是异步发送，所以这里不sleep，则消息还未发送就会将producer给关闭，报错
TimeUnit.SECONDS.sleep(3);
producer.shutdown();
```



#### 定义单向消息发送生产者

```java
DefaultMQProducer producer = new DefaultMQProducer("pg");
producer.setNamesrvAddr("rocketmqOS:9876");
producer.start();

for(int i = 0; i < 10; i++) {
    byte[] body = ("Hi," + i).getBytes();
    Message msg = new Message("single", "someTag", body);
    // 单向发送
    producer.sendOneway(msg);
}
producet.shutdown();
System.out.println("producer shutdown");
```



#### 定义消息消费者

```java
// 定义一个push消费者
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("pg");
// 指定nameServer
consumer.setNamesrvAddr("rocketmqOS:9876");
// 指定从第一条消息开始消费
consumer.setConsumeFromWhere(ConsumerFromWhere.CONSUME_FROM_FIRST_OFFSET);
// 指定消费Topic与Tag
consumer.subscribe("someTopic", "*");
// 指定采用“广播模式”进行消费，默认为“集群模式”
// consumer.setMessageModel(MessageModel.BROADCASTING);
// 注册消息监听器
consumer.registerMessageListener(new MessageListenerConcurrently() {
    // 一旦Broker中有了其订阅的消息就会触发该方法的执行。
    // 其返回值为当前Consumer消费的状态
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        //逐条消费消息
        for (MessageExt msg: msgs) {
            System.out.println(msg);
        }
        // 返回消费状态：消费成功
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
});
// 开启消费者消费
consumer.start();

TimeUnit.SECONDS.sleep();

consumer.shutdown();
```



## 二、顺序消息



### 1.什么是顺序消息

顺序消息指的是，严格按照消息的==发送顺序==进行消费的消息（FIFO）。

默认情况下生产者会把消息以Round Robin==轮询==方式发送到==不同==的Queue分区队列；而消费消息时会从多个Queue上拉取消息，这种情况下的发送和消费是不能保证顺序的。如果将消息发送到==同一个==Queue中，消费时也只从这个Queue上拉取消息，就严格保证了消息的顺序性。

>对于这种只有单个Queue的Topic，最终的消费组中也只能有一个消费者同时在消费。



### 2.为什么需要顺序消息

例如：现在有TOPIC `ORDER_STATUS`，其下有4个Queue队列，该Topic中的不同消息用于描述当前订单的不同状态。假设订单有状态：未支付、已支付、发货中、发货成功、发货失败。



根据以上订单状态，生产者从时序上可以生成如下几个消息：

订单T0000001：未支付-->订单T0000001：已支付-->订单T0000001：发货中-->订单T0000001：发货失败。

消息发送到MQ之后，Queue的选择如果采用轮询策略，消息在MQ的存储可能如下：



这种情况下，我们希望Consumer消费消息的顺序和我们发送是一致的，然而上述MQ的投递和消费方式，我们无法保证顺序是正确的。对于顺序异常的消息，Consumer即使设置有一定的状态容错，也不能完全处理好这么多种随机出现组合情况。



基于上述的情况，可以设计如下方案：对于相同订单的消息，通过一定的策略，将其放置在一个Queue中，然后消费者在采用一定的策略（例如，一个线程独立处理一个Queue，保证处理消息的顺序性），能够保证消费的顺序性。



### 3.有序性分类

根据有序范围的不同，RocketMQ可以严格保证两种消息的有序性：==分区有序==和==全局有序==。



#### 分区有序

如果有多个Queue参与，其仅可保证在该Queue分区队列上的消息顺序，则成为分区有序。

> 如何实现Queue的选择？
>
> 在定义Producer时我们可以指定消息队列选择器，而这个选择器是我们自己实现了==MessageQueueSelector==接口定义的。
>
> 在定义选择器的选择算法时，一般需要使用==选择key==。这个选择key可以是消息key。也可以是其他数据。但无论是谁做选择key，都不能重复，都是唯一的。
>
> 一般性的选择算法是，就是让选择key（或其hash值）与该Topic所包含的Queue的数量取模。其结果即为选择出的Queue的queueId。
>
> 取模算法存在一个问题：不同选择key与Queue数量取模结果可能会是相同的，即不同选择key的消息可能会出现在相同的Queue中，即同一个Consumer可能会消费到不同选择key的消息。这个问题如何解决？
>
> 一般性的做法是从消息中获取到选择key，对其进行判断，若是当前Consumer需要消费的消息，则直接消费，否则什么也不做。这种做法要求==选择key要能够随着消息一起被Consumer获取到==。此时使用消息key作为选择key是比较好的做法。
>
> 以上做法会不会如下出现新的问题？不属于那个Consumer的消息被拉取走了，应该消费该消息的Consumer是否还能再消费该消息呢？同一个Queue的消息不可能被同一个Group中的不同Consumer同时消费，所以消费同一个Queue的不同选择key的消息的Consumer一定不属于不同的Group。而不同的Group中的Consumer间的消息是相互隔离的，互不影响的。



#### 全局有序

当发送和消费参与的Queue只有一个时所保证的有序是整个Topic中消息的顺序，成为全局有序。

即整个Topic中只有一个Queue。

> 在创建Topic时，指定Queue的数量，有三种指定方式：
>
> 1. 在代码中创建Producer时，可以指定其自动创建的Topic的Queue数量。
> 2. 在RocketMQ可视化控制台中手动创建Topic时指定Queue数量。
> 3. 使用mqadmin命令手动创建Topic时指定Queue数量。



### 4.代码举例

```java
DefaultMQProducer producer = new DefaultMQProducer("pg");
producer.setNamesrvAddr("rocketmqOS:9876");
// 若为全局有序，则需要设置
producer.setDefaultTopicQueueNums(1);
producer.start();

for (int i = 0; i < 100; i++) {
	Integer orderId = i;
	byte[] body = ("Hi," + i).getBytes();
 	Message msg = new Message("TopicA", "TagA", body);
    // 将orderId作为消息key
    msg.setKeys(orderId.toString());
    // send()的第三个参数值会传递给选择器的select()的第三个参数
    // 该send()为同步发送
 	SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
        // 具体的选择算法在该方法中定义
 		@Override
 		public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
            // 以下是使用消息key作为选择key的选择算法
            String keys = msg.getKeys();
            Integer id = Integer.valueOf(keys);
            // 以下是使用arg作为选择key的选择算法
 			Integer id = (Integer) arg;
 			int index = id % mqs.size();
 			return mqs.get(index);           
 		}
 	}, orderId);
    
    System.out.println(sendResult);
}

producer.shutdown();
```



## 三、延时消息



### 1.什么是延迟消息

当消息写入到Broker后，在==指定的时长==后才可被消费处理的消息，成为延时消息。

采用RocketMQ的延时消息可以实现==定时任务==的功能，而无需使用定时器。典型的应用场景是，电商交易中超时未支付关闭订单的场景，12306平台订单超时未支付取消订票的场景。

> 在电商平台中，订单创建时会发送一条延迟消息。这条消息将会在30分钟后投递给后台业务系统（Consumer），后台业务系统收到该消息后会判断对应的订单是否已经完成支持。如果未完成，则取消订单，将商品再次放回到库存；如果完成支付，则忽略。
>
> 在12306平台中，车票预订成功后就会发送一条延迟消息。这条消息将会在45分钟后投递给后台业务系统（Consumer），后台业务系统收到该消息后会判断对应的订单是否已经完成支持。如果未完成，则取消订单，则取消预订，将车票再次放回到票池；如果完成支付，则忽略。





### 2.延时等级

延时消息的延迟时长==不支持随意时长的延迟==，是通过==特定的延迟等级==来指定的。延时等级定义在RocketMQ服务端的`MessageStoreConfig`类中的如下变量中：

```java
private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
```

即，若指定的延时等级为3，则表示延迟时长为10s。即延迟等级是从==1==开始计数的。

当然，如果需要自定义的延时等级，可以通过在broker的配置文件中新增如下配置（例如下面增加了1天这个等级1d），配置文件在RocketMQ安装目录下的conf目录中。

```
messageDelayLevel = 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h 1d
```



### 3.延迟消息实现原理

具体实现方案是：

#### 修改消息

Producer把消息发送到Broker后，Broker首先将消息写入到commitlog文件，然后需要将其分发相应的consumequeue。不过，在分发之前，系统会判断消息中==是否带有延迟等级==。若没有，则直接正常分发；若有则需要经历一个复杂的过程。

- 修改消息的Topic为==SCHEDULE_TOPIC_XXXX==

- 根据延时等级，在consumequeue目录中SCHEDULE_TOPIC_XXXX主题下创建出相应的queueId目录与consumequeue文件（如果没有这些目录与文件的话）

  > 延迟等级delayLevel与queueId的对应关系为queueId=delayLevel-1。
  >
  > 需要注意，在创建queueId目录时，并不是一次性地将所有延迟等级对应的目录全部创建完毕，而是用到哪个延迟等级创建哪个目录。

- ==修改消息索引单元内容==。索引单元中的Message Tag HashCode部分原本存放的是消息的Tag的Hash值。现修改为消息的==投递时间==，投递时间是指该消息被重新修改为原Topic后再次被写入到commitlog中的时间。==投递时间=消息存储时间+延迟等级时间==。消息存储时间是指消息被发送到Broker时的时间戳。

- 将消息索引写入到SCHEDULE_TOPIC_XXXX主题下相应的consumequeue中。

> SCHEDULE_TOPIC_XXXX目录中各个延时等级Queue中的消息是如何排序的？
>
> 是按照消息投递时间排序的。一个Broker中同一等级的所有延迟消息会被写入到consumequeue目录中SCHEDULE_TOPIC_XXXX目录下相同Queue中。即一个Queue中消息投递时间的延迟等级时间是相同的。那么投递时间就取决于==消息存储时间==。即按照消息被==发送到Broker的时间==进行排序的。



#### 投递延迟消息

Broker内部有一个延迟消息服务类`ScheduleMessageService`，其会消费SCHEDULE_TOPIC_XXXX中的消息，即按照每条消息的投递时间，将延时消息投递到目标Topic中。不过，在投递之前会从commitlog中将原来写入的消息再次读出，并将其原来的延时消息等级设置为0，即原消息变为了一条不延迟的普通消息。然后再次将消息投递到目标Topic中。

> ScheduleMessageService在Broker启动时，会创建并启动一个定时器Timer，用于执行相应的定时任务。系统会根据延时等级的个数，定义相应数量的TimerTask，每个TimerTask负责一个延迟等级消息的消费与投递。==每个TimerTask都会检测相应Queue队列的第一条消息是否到期==。若第一条消息没有到期，则后面的所有消息更不会到期（消息是按照投递时间排序的）；若第一条消息到期了，则将该消息投递到目标Topic，即消费该消息。



#### 将消息重新写入commitlog

延迟消息服务类`ScheduleMessageService`将延迟消息再次发送给了commitlog，并再次形成新的消息索引条目，分发到相应Queue。

> 这其实就是一次普通消息发送，只不过这次的消息Producer是延迟消息服务类ScheduleMessageService。



### 4.代码示例



#### 定义DelayProducer类

```java
DefaultMQProducer producer = new DefaultMQProducer("pg");
producer.setNamesrvAddr("rocketmqOS:9876");
producer.start();

for(int i = 0; i < 10; i++) {
    byte[] body = ("Hi," + i).getBytes();
    Message msg = new Message("TopicB", "someTag", body);
    // 指定消息延迟等级为3级，即延迟10s
    msg.setDelayLevel(3);
    SendResult sendResult = producer.send(msg);
    // 输出消息被发送的时间
    System.out.print(new SimpleDateFormat("mm:ss").format(new Date()));
    System.out.println(" ," + sendResult);
}

producet.shutdown();
```



#### 定义OtherConsumer者

```java
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("pg");
consumer.setNamesrvAddr("rocketmqOS:9876");

consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
consumer.subscribe("TopicB", "*");
consumer.registerMessageListener(new MessageListenerConcurrently(){
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for(MessageExt msg: msgs) {
    		System.out.print(new SimpleDateFormat("mm:ss").format(new Date()));
            System.out.println(" ," + msg);
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
});
consumer.start();
```



## 四、事务消息



### 1.问题引入

这里的一个需求场景是：工行用户A向建行用户B转账1万元。

我们可以使用同步消息来处理该需求场景：

1. 工行系统发送一个给B增款1万元的同步消息M给Broker
2. 消息被Broker成功接收后，向工行系统发送成功ACK
3. 工行系统收到成功ACK后从用户A中扣款1万元
4. 建行系统从Broker中获取到消息M
5. 建行系统消费消息M，即向用户B中增加1万元

> 这其中是有问题的，若第三步中的扣款操作失败，但消息已经成功发送到了Broker。对于MQ来说，只要消息写入成功，那么这个消息就可以被消费。此时建行系统中用户B增加了1万元。出现了数据不一致问题。



### 2.解决思路

解决思路是，让第1、2、3步具有原子性，要么全部成功，要么全部失败。即消息发送成功后，必须要保证扣款成功。如果扣款失败，则回滚发送成功的消息。而该思路即使用==事务消息==。这里要使用==分布式事务==解决方案。



### 3.基础



### 4.XA模式三剑客





### 5.





# 第五章 请求头



## 一、Producer



### 1.消息相关

| 描述         | 接受端 | 请求码 | 相关类                                                       |
| ------------ | ------ | ------ | ------------------------------------------------------------ |
| 发送消息     | Broker | 10     | RequestCode#SEND_MESSAGE<br />SendMessageRequestHeader<br />SendMessageProcessor<br />SendMessageResponseHeader |
| 发送消息V2   | Broker | 310    | RequestCode#SEND_MESSAGE_V2<br />SendMessageRequestHeaderV2<br />SendMessageProcessor<br />SendMessageResponseHeader |
| 发送批次消息 | Broker | 320    | RequestCode#SEND_BATCH_MESSAGE<br />SendMessageRequestHeaderV2<br />SendMessageProcessor<br />SendMessageResponseHeader |



### 2.路由信息相关

| 描述               | 接受端  | 请求码 | 相关类                                                       |
| ------------------ | ------- | ------ | ------------------------------------------------------------ |
| 获取主题的路由信息 | Namesrv | 105    | RequestCode#GET_ROUTEINFO_BY_TOPIC<br />GetRouteInfoRequestHeader<br />ClientRequestProcessor<br />RemotingCommand |
|                    |         |        |                                                              |
|                    |         |        |                                                              |







## 二、Consumer





## 三、Broker



### 1.注册相关

| 描述          | 接受端  | 请求码 | 相关类                      |
| ------------- | ------- | ------ | --------------------------- |
| 注册到Namesrv | Namesrv | 103    | RequestCode#REGISTER_BROKER |
|               |         |        |                             |
|               |         |        |                             |







## 四、Namesrv

