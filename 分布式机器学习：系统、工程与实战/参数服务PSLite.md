# 参数服务器 PS-Lite

参数服务器解决的问题是：**模型参数太大、数据太多、单机放不下或算不动时，如何把“存参数”和“算梯度”拆到多台机器上协同完成。**

整篇可以用一条链串起来：

```text
单机训练：本地保存参数 + 本地算梯度 + 本地更新参数
  ↓
分布式训练：参数存储和梯度计算拆开
  ↓
Server 存参数，Worker 算梯度
  ↓
Worker pull 参数，push 梯度
  ↓
多个 Server 分片存参数，需要路由 key
  ↓
节点多了，需要 Scheduler 管注册、心跳、同步和重连
  ↓
PS-Lite 用 Postoffice、Van、Customer、KVWorker、KVServer 分层实现
```

理解 PS-Lite 的关键不是先背类名，而是抓住五个问题：**参数怎么存、梯度怎么传、节点怎么找、消息怎么送、请求怎么等。**

---

## 1. 从单机训练到参数服务器

单机训练的四步：

```text
准备数据和参数
  ↓
前向计算 loss
  ↓
反向计算 grad
  ↓
更新 weight
```

参数服务器把这件事拆成两类角色：

| 角色 | 职责 |
|------|------|
| Worker | 拿数据，拉参数，算前向/反向，推梯度 |
| Server | 存参数，收梯度，更新参数，应答 pull |
| Scheduler | 管节点注册、心跳、同步、重连 |

训练循环变成：

```text
Worker: pull 参数
  ↓
Worker: 前向 + 反向，算本地梯度
  ↓
Worker: push 梯度给 Server
  ↓
Server: 聚合梯度，更新参数
  ↓
下一轮 Worker 再 pull 新参数
```

本质变化：

```text
单机：一个进程同时管参数、数据、计算、更新
PS：Server 管参数和更新，Worker 管数据和计算
```

拆开的好处是能扩展规模；代价是必须解决通信、路由、同步和容错。

---

## 2. Server 为什么像分布式 KV 存储

参数服务器通常把参数抽象成 key-value：

```text
key   = 参数 id / 特征 id
value = 权重、向量、矩阵
```

这样做特别适合推荐系统、CTR 这类稀疏场景：一个样本只访问少量特征，Worker 只需要 pull 本轮用到的 key，不必拉完整模型。

Server 的核心能力：

| 能力 | 说明 |
|------|------|
| KV 存储 | 按 key 保存参数 |
| 模型分片 | 多个 Server 分别保存不同 key 范围 |
| push/pull | Worker 用 push 发梯度，用 pull 拉参数 |
| 参数更新 | Server 收到梯度后执行 SGD、Adam 等更新逻辑 |

分片示意：

```text
Server0: key [0, 1000)
Server1: key [1000, 2000)
Server2: key [2000, 3000)

Worker pull(key=1500) → 发给 Server1
Worker push(key=500)  → 发给 Server0
```

参数服务器和 AllReduce 的区别：

| 维度 | 参数服务器 | AllReduce |
|------|------------|-----------|
| 参数位置 | 主要放在 Server | 每个 Worker 都有完整模型 |
| 通信方式 | 按 key push/pull | 所有节点共同归约张量 |
| 适合场景 | 稀疏参数、推荐系统 | 稠密梯度、大模型数据并行 |
| 主要风险 | Server 可能成为瓶颈 | 同步等待、慢节点拖累 |

结论：**PS 不一定过时，它更适合稀疏、大规模 key-value 参数；AllReduce 更适合稠密大张量同步。**

---

## 3. 参数服务器的优点和痛点

参数服务器的优势：

- 参数可以分片到多个 Server，模型容量更大。
- Worker 可以并行计算，吞吐更高。
- 支持同步、异步、半同步等训练策略。
- 对稀疏参数友好，只通信本轮访问到的 key。

主要痛点：

| 痛点 | 原因 |
|------|------|
| 网络瓶颈 | 大量 Worker 都向 Server push/pull |
| 比例难调 | Worker 和 Server 数量需要经验调参 |
| 编程复杂 | 要处理 push/pull、clock、barrier、路由等概念 |
| 额外成本 | Server 往往需要额外机器或资源 |

所以稠密大模型训练更常用 NCCL + AllReduce；但推荐系统、广告 CTR 这类高维稀疏场景仍然常用参数服务器。

---

## 4. PS-Lite 的核心模块

PS-Lite 的设计目标是把参数服务器拆成清晰几层：

```text
业务层：KVWorker / KVServer
请求层：SimpleApp / Customer
通信层：Van
管理层：Postoffice
环境层：Environment
```

各模块解决的问题：

| 问题 | 模块 |
|------|------|
| 进程是什么角色 | Environment |
| 节点是谁、Server 在哪、key 发给谁 | Postoffice |
| 消息怎么跨机器发送 | Van |
| 一次请求要等几个回复 | Customer |
| 用户怎么写 push/pull 逻辑 | SimpleApp |
| 参数服务器业务接口 | KVWorker / KVServer |

模块关系：

```text
KVWorker / KVServer
        ↓
SimpleApp
        ↓
Customer
        ↓
Van
        ↓
网络

Postoffice 负责节点、路由、Barrier、心跳等全局信息
```

最重要的分工：

- `KVWorker`：Worker 侧入口，负责 `Push()`、`Pull()`、`Wait()`。
- `KVServer`：Server 侧入口，负责处理 Worker 请求并更新参数。
- `Customer`：追踪请求和回复，维护消息队列。
- `Van`：真正收发网络消息。
- `Postoffice`：全局节点管理器，知道自己是谁、别人在哪、key 该去哪。

---

## 5. 启动流程：先注册，再通信

每个进程通过环境变量决定角色：

```text
DMLC_ROLE=scheduler → Scheduler
DMLC_ROLE=server    → Server
DMLC_ROLE=worker    → Worker
```

启动时，每个 Worker/Server 一开始只知道 Scheduler 地址，不知道其他节点在哪。所以流程是：

```text
1. Scheduler 先启动，等待注册
2. Worker / Server 启动，连接 Scheduler
3. Worker / Server 发送 ADD_NODE 注册自己
4. Scheduler 收齐节点后分配 id / rank
5. Scheduler 把完整地址簿发给所有节点
6. Worker 和 Server 根据地址簿直接建立连接
7. Barrier 确保所有节点就绪，再开始训练
```

启动阶段和训练阶段的数据流不同：

```text
启动阶段：Worker/Server → Scheduler → 分发节点信息
训练阶段：Worker ↔ Server 直接 push/pull
```

Scheduler 主要是控制面，不参与正常训练的数据流。

---

## 6. Postoffice：节点的大管家

Postoffice 负责回答这些问题：

```text
我是谁？
集群里有哪些节点？
Server 在哪里？
某个 key 应该发给哪个 Server？
哪些节点到达 Barrier 了？
哪些节点可能挂了？
```

核心功能：

| 功能 | 说明 |
|------|------|
| 节点管理 | 保存节点 id、角色、地址 |
| 路由 | 根据 key 找到负责的 Server |
| Barrier | 等待一组节点到齐再放行 |
| 心跳 | 记录节点是否存活 |
| 持有 Van | 通过 Van 做实际通信 |

### 节点寻址

PS-Lite 用整数 id 同时表示“组”和“具体节点”：

| 对象 | id |
|------|----|
| Scheduler 组 | 1 |
| Server 组 | 2 |
| Worker 组 | 4 |
| 所有人 | 7 = 1 + 2 + 4 |
| 具体 Server | rank * 2 + 8 |
| 具体 Worker | rank * 2 + 9 |

这样一个 int 就能表达发给单个节点、某个组、多个组。

---

## 7. 路由机制：key 该发给谁

多个 Server 分片存参数，所以 Worker 必须知道每个 key 属于哪个 Server。

```text
Server0: [0, 1000)
Server1: [1000, 2000)
Server2: [2000, 3000)

push key=1500 → Server1
pull key=500  → Server0
```

PS-Lite 的做法是：**Worker 端根据 key 范围切片，然后分别发给对应 Server。**

```text
Worker keys: [0, 100, 1500, 2500]
  ↓
发给 Server0: [0, 100]
发给 Server1: [1500]
发给 Server2: [2500]
```

好处是 Server 不用判断“这个 key 是不是我的”，Worker 直接把请求发到正确位置。

---

## 8. Van：通信层

Postoffice 决定“发给谁”，Van 负责“怎么发出去”。

Van 的职责：

| 职责 | 说明 |
|------|------|
| 建立连接 | Worker、Server、Scheduler 之间建连 |
| 收发消息 | 基于 ZeroMQ 收发网络消息 |
| 接收线程 | 持续监听消息并分发 |
| 心跳线程 | Worker/Server 定期向 Scheduler 报活 |
| 重传线程 | 启用时用 ACK + 超时重传保证可靠性 |

消息类型分两类：

```text
控制消息：ADD_NODE / BARRIER / HEARTBEAT / TERMINATE
数据消息：push / pull / response
```

Van 不处理训练业务，只负责把消息分发给正确的 Customer 或控制处理函数。

---

## 9. Barrier、心跳和重传

### Barrier

Barrier 解决“所有节点是否都走到同一阶段”的问题。

```text
Worker1 到达 Barrier ┐
Worker2 到达 Barrier ┼→ Scheduler 计数，全到齐后统一放行
Worker3 到达 Barrier ┘
```

常见用途：初始化完成后统一开始、某些同步阶段统一推进、退出前同步。

### 心跳

Worker/Server 定期给 Scheduler 发心跳：

```text
Worker/Server → HEARTBEAT → Scheduler
Scheduler 更新 heartbeats_[node_id] = now
```

如果长时间没收到心跳，Scheduler 将节点标记为 dead。节点重启后重新发送 `ADD_NODE` 接入集群。

### 重传

Resender 用三步保证可靠性：

```text
发送方缓存消息
  ↓
接收方收到后回复 ACK
  ↓
发送方收到 ACK 后删除缓存；超时没收到就重发
```

核心就是：**缓存 + ACK + 超时重传。**

---

## 10. Customer 和 SimpleApp

Van 只负责网络，不应该直接执行业务逻辑。数据消息进入节点后，会交给 Customer。

### Customer

Customer 的职责：

| 功能 | 说明 |
|------|------|
| 消息队列 | Van 收到消息后放入 `recv_queue_` |
| 请求追踪 | 记录每个请求要等几个回复 |
| 回调处理 | 收到消息后调用注册的 `recv_handle_` |

请求追踪示意：

```cpp
tracker_[request_id] = {expected_count, received_count};
```

例如一次 pull 发给 3 个 Server，就要等 3 个回复都回来，`Wait(timestamp)` 才能结束。

### SimpleApp

SimpleApp 是业务抽象层，负责区分请求和应答：

```cpp
if (msg.meta.request) {
    request_handle_(msg);
} else {
    response_handle_(msg);
}
```

KVWorker 和 KVServer 都建立在 SimpleApp 上。

---

## 11. KVWorker 和 KVServer

### KVWorker

KVWorker 是 Worker 侧接口：

| 函数 | 作用 |
|------|------|
| `Push(keys, grads)` | 把梯度按 key 切片后发给 Server |
| `Pull(keys)` | 从对应 Server 拉参数 |
| `Wait(timestamp)` | 等待一次请求的所有回复 |
| `DefaultSlicer()` | 根据 key 范围切分请求 |

典型流程：

```text
Pull(keys) → Wait
前向 + 反向
Push(keys, grads) → Wait
下一轮
```

### KVServer

KVServer 是 Server 侧接口：

```text
收到 Worker 请求
  ↓
KVServer::Process()
  ↓
调用用户注册的 request_handle_
  ↓
更新参数或返回参数
  ↓
Response() 回复 Worker
```

用户真正需要关心的是 `request_handle_`：收到梯度后怎么聚合、怎么更新参数、什么时候回复。

---

## 12. 一次 push/pull 消息怎么走

从 Worker 到 Server：

```text
KVWorker::Push/Pull
  ↓
Customer::NewRequest 记录要等几个回复
  ↓
Van::Send 发到网络
  ↓
Server 侧 Van::Receiving 收到
  ↓
ProcessDataMsg 找到 Customer
  ↓
Customer 放入 recv_queue_
  ↓
KVServer::Process 处理请求
  ↓
request_handle_ 更新参数或准备返回值
  ↓
Response 发回 Worker
```

Server 回复 Worker：

```text
Worker 侧 Van 收到 response
  ↓
ProcessDataMsg → Customer
  ↓
Customer 更新 tracker_
  ↓
如果所有回复都到了，Wait(timestamp) 结束
```

一句话：**Van 管传输，Customer 管等待，KVWorker/KVServer 管业务。**

---

## 13. 总结

PS-Lite 的因果主线：

```text
参数和数据变大
  ↓
拆成 Server 存参数、Worker 算梯度
  ↓
用 push/pull 交换参数和梯度
  ↓
参数按 key 分片，所以需要路由
  ↓
节点需要注册、同步、心跳、重连，所以需要 Scheduler/Postoffice
  ↓
消息需要网络传输，所以需要 Van
  ↓
请求需要异步等待和追踪，所以需要 Customer
  ↓
最终暴露 KVWorker/KVServer 给用户写训练逻辑
```

核心结论：

| 问题 | PS-Lite 的回答 |
|------|----------------|
| 参数放哪里 | 多个 Server 按 key 分片保存 |
| Worker 怎么拿参数 | `Pull(keys)` |
| Worker 怎么交梯度 | `Push(keys, grads)` |
| key 发给谁 | Worker 根据 key range 做路由 |
| 节点怎么发现 | 向 Scheduler 发送 `ADD_NODE` |
| 节点怎么同步 | `Barrier` |
| 节点怎么保活 | 心跳机制 |
| 消息怎么可靠 | ACK + 超时重传 |
| 网络和业务怎么解耦 | Van 管网络，Customer 管请求，KV 层管业务 |

一句话收束：**PS-Lite 是一个分层清晰的参数服务器实现：参数按 key 存，梯度按 key 传，节点由 Postoffice 管，消息由 Van 送，请求由 Customer 跟踪，最终由 KVWorker/KVServer 提供 push/pull 接口。**