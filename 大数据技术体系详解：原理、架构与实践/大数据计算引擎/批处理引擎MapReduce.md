# 批处理引擎 MapReduce

MapReduce 解决的问题是：**当数据大到单机放不下、算不动时，如何让普通工程师不用手写分布式通信、容错和调度，也能处理海量数据。**

它的核心思想可以压缩成一句话：

> 用户只写 `Map` 和 `Reduce` 两段业务逻辑；框架负责数据切分、任务调度、Shuffle、排序、容错和输出。

整篇笔记可以用一条因果链串起来：

```text
搜索引擎需要处理海量网页
  ↓
单机存储和计算都不够
  ↓
HDFS/GFS 解决“怎么存”
  ↓
MapReduce 解决“怎么批量算”
  ↓
用户只写 Map/Reduce，框架隐藏分布式细节
  ↓
InputFormat、Partitioner、Shuffle、OutputFormat 共同组成运行骨架
  ↓
YARN 负责资源调度，MapTask/ReduceTask 负责具体执行
  ↓
数据本地性、推测执行、压缩、Combiner 等机制提升吞吐和稳定性
```

理解 MapReduce，不要先背 API，而要先抓住三个问题：

```text
1. 大文件怎么切成很多小任务？
2. Map 产生的同 key 数据怎么汇聚到一起？
3. 机器挂了、任务慢了、数据太多了，框架怎么兜底？
```

---

## 1. 为什么会有 MapReduce

Hadoop MapReduce 的起点不是“为了造一个框架”，而是搜索引擎遇到了实际瓶颈。

```text
2002 Nutch 想做开源搜索引擎
  ↓
网页数量增长，单机存不下，也索引不动
  ↓
Google 论文给出两个关键答案：
  2003 GFS：海量数据怎么存
  2004 MapReduce：海量数据怎么批量算
  ↓
Nutch 团队实现 NDFS + MapReduce
  ↓
2006 从 Nutch 拆出成为 Hadoop
  ↓
2008 Hadoop 成为 Apache 顶级项目
```

这个历史说明了一件事：MapReduce 是为离线大规模数据处理而生的，典型场景包括：

- 搜索引擎索引构建
- 日志分析
- 离线统计
- ETL 数据清洗
- 大规模排序和聚合

它追求的不是低延迟，而是：**高吞吐、可扩展、能容错、易编程。**

---

## 2. MapReduce 的设计目标

MapReduce 的四个目标都来自分布式批处理的痛点。

| 目标 | 痛点 | MapReduce 的做法 |
|------|------|------------------|
| 易编程 | 手写 MPI 要管切分、通信、容错 | 用户只写 Map 和 Reduce |
| 可扩展 | 数据量变大，单机扛不住 | 加机器、加任务并行处理 |
| 高容错 | 大集群里机器故障是常态 | 任务失败自动重试 |
| 高吞吐 | 离线批处理更关心总处理量 | 牺牲延迟，优化吞吐 |

它把“做什么”和“怎么做”分开：

```text
用户负责：业务逻辑，也就是 map() 和 reduce()
框架负责：输入切分、调度、Shuffle、排序、容错、输出
```

---

## 3. WordCount：为什么用户只写两个函数

WordCount 是 MapReduce 的经典例子。目标是统计每个单词出现次数。

如果自己写分布式程序，需要处理：

| 问题 | 自己写会遇到什么 |
|------|------------------|
| 数据切分 | 文件怎么切成多份，分给哪些机器 |
| 并行计算 | 每台机器处理哪一块数据 |
| 数据汇聚 | 相同单词的结果怎么汇总到一起 |
| 故障恢复 | 某台机器挂了怎么办 |
| 扩展性 | 加机器后代码和调度怎么变 |

MapReduce 把这些都放进框架，用户只写：

```java
map(key, value):
    // value 是一行文本
    for word in value.split():
        output(word, 1)

reduce(key, values):
    // key 是单词，values 是这个单词对应的所有 1
    output(key, sum(values))
```

完整数据流：

```text
输入文本：
hello world
hello hadoop

Map 输出：
<hello, 1>
<world, 1>
<hello, 1>
<hadoop, 1>

Shuffle + Sort 后：
<hello, [1, 1]>
<hadoop, [1]>
<world, [1]>

Reduce 输出：
<hello, 2>
<hadoop, 1>
<world, 1>
```

一句话：**MapReduce 把“分而治之”固化成 Map 和 Reduce 两个阶段，框架自动完成中间的分发、排序和容错。**

---

## 4. 数据模型：一切都是 `<key, value>`

MapReduce 统一用 `<key, value>` 表达输入、中间结果和输出。

```text
HDFS 文件
  ↓
InputFormat 切成 split
  ↓
RecordReader 解析成 <k, v>
  ↓
Mapper 输出中间 <k, v>
  ↓
Shuffle + Sort 按 key 分组
  ↓
Reducer 输出最终 <k, v>
  ↓
OutputFormat 写到 HDFS
```

几个关键概念：

| 概念 | 含义 |
|------|------|
| Block | HDFS 的物理存储单位，常见 128MB |
| Split | MapReduce 的逻辑计算单位 |
| Map Task | 一个 split 通常对应一个 Map Task |
| RecordReader | 把 split 解析成一条条 `<key, value>` |

Block 和 Split 的区别：

```text
Block：数据在 HDFS 上怎么存，是物理概念
Split：MapReduce 怎么切任务，是逻辑概念
```

通常一个 Block 对应一个 Split，但二者不是同一件事。

---

## 5. 五步计算模型和五个组件

MapReduce 的计算骨架：

```text
InputFormat
  ↓
Mapper
  ↓
Partitioner
  ↓
Shuffle + Sort
  ↓
Reducer
  ↓
OutputFormat
```

五个可编程组件：

| 组件 | 作用 | 用户是否必须写 |
|------|------|----------------|
| InputFormat | 切 split，解析输入 `<k,v>` | 通常用默认 |
| Mapper | 处理输入，输出中间 `<k,v>` | 必须 |
| Partitioner | 决定 key 发给哪个 Reducer | 通常用默认 |
| Reducer | 对同 key 的 values 做归约 | 常见需要，纯 Map 可省略 |
| OutputFormat | 把最终结果写出 | 通常用默认 |

可选组件：

| 组件 | 作用 |
|------|------|
| Combiner | Map 端本地预聚合，减少 Shuffle 数据量 |
| Counter | 统计处理过程中的指标，如脏数据条数 |

设计思想：**框架提供骨架，用户只填业务逻辑。**

---

## 6. Mapper 和 Reducer

### Mapper

Mapper 的本质：

```text
输入：<KEYIN, VALUEIN>
  ↓ map()
输出：<KEYOUT, VALUEOUT>
```

WordCount 里：

| 泛型参数 | 含义 | 例子 |
|----------|------|------|
| KEYIN | 输入 key | LongWritable，行偏移量 |
| VALUEIN | 输入 value | Text，一行文本 |
| KEYOUT | 输出 key | Text，单词 |
| VALUEOUT | 输出 value | IntWritable，次数 1 |

Hadoop 要求 key/value 可序列化，因为中间结果可能写磁盘，也可能网络传输。常用类型：

| Java 类型 | Hadoop 类型 |
|-----------|-------------|
| int | IntWritable |
| long | LongWritable |
| String | Text |
| byte[] | BytesWritable |

### Reducer

Reducer 的本质：

```text
输入：<key, [v1, v2, ...]>
  ↓ reduce()
输出：<key, result>
```

WordCount 里：

```java
reduce(word, [1, 1, 1]):
    output(word, 3)
```

Mapper 和 Reducer 的区别：

| 维度 | Mapper | Reducer |
|------|--------|---------|
| 输入 | split 解析出的 `<k,v>` | Shuffle 后的 `<k, [v...]>` |
| 主要工作 | 提取、过滤、转换 | 聚合、归并、汇总 |
| 并行度 | 通常由 split 数决定 | 用户配置 Reduce Task 数 |
| 输出 | 中间结果 | 最终结果 |

一句话：**Mapper 负责把数据拆成可聚合的中间 KV，Reducer 负责把同 key 的数据合并成最终结果。**

---

## 7. InputFormat：数据怎么进来

InputFormat 解决两个问题：

| 问题 | 说明 |
|------|------|
| 怎么切 | 把输入切成多个 split，决定 Map Task 数量 |
| 怎么读 | 把 split 解析成 `<key, value>` 流 |

常见 InputFormat：

| InputFormat | 适用场景 | 输出 |
|-------------|----------|------|
| TextInputFormat | 普通文本、日志、CSV | `<行偏移量, 行内容>` |
| KeyValueTextInputFormat | 每行天然是 key/value | `<key, value>` |
| SequenceFileInputFormat | Hadoop 二进制 KV 文件 | `<key, value>` |
| NLineInputFormat | 按固定行数切 split | `<偏移量, 行内容>` |
| DBInputFormat | 关系数据库 | 数据库记录 |

TextInputFormat 示例：

```text
文件内容：
hello world
hello hadoop

解析结果：
<0,  "hello world">
<12, "hello hadoop">
```

FileInputFormat 的设计是模板方法：

```text
FileInputFormat：负责通用切分逻辑 getSplits()
子类：负责具体读取方式 createRecordReader()
```

也就是：**基类管“怎么切”，子类管“怎么读”。**

---

## 8. Partitioner：key 发给哪个 Reducer

Mapper 输出很多 `<key, value>`，问题是：每条记录该发给哪个 Reducer？这就是 Partitioner 的职责。

默认 HashPartitioner：

```java
(key.hashCode() & Integer.MAX_VALUE) % numReduceTasks
```

它保证：**同一个 key 一定进入同一个 Reducer。**

```text
Map 输出：
<hello, 1>
<world, 1>
<hello, 1>

Partitioner：
hello → Reducer0
world → Reducer1
hello → Reducer0
```

为什么重要？因为 Reduce 必须看到某个 key 的全部 values，才能正确聚合。

坏的分区会造成数据倾斜：

```text
Reducer0: 1 亿条
Reducer1: 100 条
Reducer2: 80 条
```

如果默认哈希不符合业务需求，可以自定义 Partitioner，比如按 `dealid` 分区、按地区分区、按业务字段分区。

---

## 9. Shuffle + Sort：MapReduce 的核心

Shuffle 是 MapReduce 最关键、也最重的阶段。它解决的问题是：**把 Mapper 输出中相同 key 的数据送到同一个 Reducer，并排好序、分好组。**

```text
Map 输出：
<a, 1>, <b, 1>, <a, 1>, <c, 1>, <b, 1>

Shuffle + Sort：
<a, [1, 1]>
<b, [1, 1]>
<c, [1]>

Reduce：
<a, 2>
<b, 2>
<c, 1>
```

它做了几件事：

| 步骤 | 作用 |
|------|------|
| Partition | 决定每条 Map 输出去哪个 Reducer |
| Spill | Map 输出先写内存缓冲，满了溢写到本地磁盘 |
| Sort | Map 端先局部排序 |
| Copy | Reduce 端从所有 Map 拉取属于自己的分片 |
| Merge | Reduce 端多路合并 |
| Group | key 相同的数据聚在一起，交给 reduce() |

MapReduce 能做大规模聚合，关键就在这里：**用排序和外部归并，把“按 key 分组”做成可落盘、可扩展的过程。**

---

## 10. Map Task 和 Reduce Task 怎么执行

### Map Task 五阶段

```text
Read → Map → Collect → Spill → Combine/Merge
```

| 阶段 | 做什么 |
|------|--------|
| Read | 读取 split，解析成 `<k,v>` |
| Map | 调用户的 `map()` |
| Collect | 将输出写入环形缓冲区，并按 Partitioner 标记分区 |
| Spill | 缓冲区满后排序并溢写到本地磁盘 |
| Combine/Merge | 合并多个 spill 文件，必要时执行 Combiner |

为什么 Map 输出要落本地磁盘？

| 方案 | 问题 |
|------|------|
| 全放内存 | 数据太大，容易爆内存 |
| 直接发给 Reduce | Reduce 可能还没启动，失败后也难恢复 |
| 本地磁盘 | 可控、可重读、方便 Reduce 拉取 |

最终每个 Map Task 通常生成：

```text
一个数据文件 + 一个索引文件
```

索引文件记录每个 Reducer 对应的数据段，Reduce 拉数据时按索引定位。

### Reduce Task 五阶段

```text
Shuffle/Copy → Merge → Sort → Reduce → Write
```

| 阶段 | 做什么 |
|------|--------|
| Copy | 从所有 Map Task 拉取属于自己的数据 |
| Merge | 合并内存和磁盘上的多个小文件 |
| Sort | 多路归并排序，让相同 key 相邻 |
| Reduce | 调用户的 `reduce()` |
| Write | 通过 OutputFormat 写到 HDFS |

Map Task 和 Reduce Task 对比：

| 维度 | Map Task | Reduce Task |
|------|----------|-------------|
| 输入 | HDFS split | 多个 Map 的中间结果 |
| 输出位置 | 本地磁盘 | HDFS |
| 排序 | Map 端局部排序 | Reduce 端归并排序 |
| 核心目的 | 生成可拉取的中间数据 | 聚合并输出最终结果 |

---

## 11. Combiner：为什么要 Map 端预聚合

Shuffle 通常是 MapReduce 最贵的阶段，因为要落盘和跨网络传输。Combiner 的作用是在 Map 端先做一次局部聚合，减少网络数据量。

WordCount 示例：

```text
没有 Combiner：
<wish, 1>, <wish, 1>, <wish, 1>, <wish, 1>

有 Combiner：
<wish, 4>
```

适合用 Combiner 的操作：

| 操作 | 是否适合 | 原因 |
|------|----------|------|
| sum | 适合 | 局部求和再全局求和正确 |
| count | 适合 | 局部计数再全局计数正确 |
| max/min | 适合 | 局部最大再全局最大正确 |
| average | 不能直接用 | 平均值的平均值不等于全局平均，需要携带 sum/count |

一句话：**Combiner 不是必然执行的 Reducer，而是框架可选择执行的 Map 端优化；逻辑必须满足局部聚合后仍然正确。**

---

## 12. MapReduce On YARN

在 Hadoop 2 之后，MapReduce 通常跑在 YARN 上。职责拆成两层：

```text
ResourceManager：全局资源调度
NodeManager：单机资源管理
MRAppMaster：某个 MapReduce 作业的管理器
MapTask/ReduceTask：真正执行计算
```

运行流程：

```text
1. 用户提交 Job 到 ResourceManager
2. ResourceManager 分配 Container 启动 MRAppMaster
3. MRAppMaster 向 ResourceManager 申请资源
4. 拿到资源后，请 NodeManager 启动 Map/Reduce Task
5. Task 执行并向 MRAppMaster 汇报进度
6. Task 失败则由 MRAppMaster 重新调度
7. 作业完成后 MRAppMaster 注销退出
```

容错：

| 谁失败 | 谁处理 | 怎么恢复 |
|--------|--------|----------|
| MapTask/ReduceTask | MRAppMaster | 重新调度任务 |
| NodeManager | ResourceManager | 标记节点失败，任务迁移 |
| MRAppMaster | ResourceManager | 重启 AppMaster，恢复作业 |

本质：**YARN 管资源，MRAppMaster 管作业，Task 管计算。**

---

## 13. 两项核心优化：数据本地性和推测执行

### 数据本地性

大数据系统的原则是：**移动计算比移动数据便宜。**

MapReduce 会尽量把 Map Task 调度到数据所在节点：

| 本地性 | 含义 | 优先级 |
|--------|------|--------|
| node-local | 任务和数据在同一节点 | 最高 |
| rack-local | 任务和数据在同一机架 | 次高 |
| off-switch | 跨机架访问数据 | 最低 |

调度策略：

```text
优先 node-local
  ↓ 等不到资源
尝试 rack-local
  ↓ 还等不到
接受 off-switch
```

这叫延迟调度：宁愿稍微等一下，也尽量让计算靠近数据。

### 推测执行

大作业经常被慢任务拖住：

```text
Map1: 100%
Map2: 100%
Map3:  45%  ← 慢任务拖住整个作业
```

推测执行的做法：

```text
发现 Map3 明显落后
  ↓
启动一个备份 Map3'
  ↓
Map3 和 Map3' 谁先完成，就采用谁的结果
```

它解决的是 Straggler 问题，和分布式训练里的慢 Worker 是同一类问题，只是 MapReduce 用“开备份任务”解决。

---

## 14. 压缩：为什么 Map 输出常常要压缩

MapReduce 有三个位置可以压缩：

```text
Map 输入
  ↓
Map 输出（中间结果，强烈建议压缩）
  ↓
Reduce 输出
```

压缩选择主要看三个指标：

| 指标 | 含义 |
|------|------|
| 压缩比 | 能压多小 |
| 压缩/解压速度 | 处理开销多大 |
| 可切分性 | 压缩文件能否拆成多个 split 并行读 |

常见算法：

| 算法 | 压缩比 | 速度 | 可切分 |
|------|--------|------|--------|
| Gzip | 高 | 中 | 否 |
| Snappy | 低 | 极快 | 否 |
| LZO | 中 | 快 | 是 |
| Bzip2 | 很高 | 慢 | 是 |

为什么可切分性重要？

```text
1GB Gzip 文件不可切分 → 只能 1 个 Map Task 处理
1GB 可切分文件 → 可以拆成多个 split 并行处理
```

经验：

- Map 输出是临时中间数据，要落盘、要网络传输，常用 Snappy 压缩。
- Reduce 输出是否压缩，要看数据后续是否频繁读取。
- 冷数据更看重压缩比，热数据更看重读写速度。

---

## 15. 新旧 API 和编程流程

Hadoop MapReduce 有旧 API 和新 API。

| 维度 | 旧 API | 新 API |
|------|--------|--------|
| 包名 | `org.apache.hadoop.mapred` | `org.apache.hadoop.mapreduce` |
| 组件定义 | 接口 | 抽象类 |
| 参数传递 | 多个零散参数 | `Context` 统一封装 |

新 API 的设计更适合演化：

```text
接口：新增方法会破坏所有实现类
抽象类：新增方法可以给默认实现，兼容旧代码
```

开发一个 Java MapReduce 作业通常三步：

```text
1. 实现 Mapper、Reducer 和 main 函数
2. 本地小数据调试
3. 提交到 Hadoop 集群处理 HDFS 数据
```

main 函数通常做这些配置：

```java
Job job = Job.getInstance(conf, "JobName");
job.setJarByClass(MyJob.class);
job.setMapperClass(MyMapper.class);
job.setReducerClass(MyReducer.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
// 设置输入路径、输出路径
// job.waitForCompletion(true)
```

---

## 16. Hadoop Streaming：非 Java 怎么写 MapReduce

Hadoop Streaming 把 Java API 变成了 Unix 管道模型：任何能从 stdin 读、往 stdout 写的程序，都可以当 Mapper/Reducer。

```text
标准 MapReduce：Java Mapper / Java Reducer
Hadoop Streaming：脚本或可执行文件 stdin → stdout
```

Python WordCount：

```python
# mapper.py
import sys
for line in sys.stdin:
    for word in line.split():
        print(f"{word}\t1")
```

```python
# reducer.py
import sys
current, count = None, 0
for line in sys.stdin:
    word, value = line.strip().split('\t')
    if word != current:
        if current is not None:
            print(f"{current}\t{count}")
        current, count = word, 0
    count += int(value)
if current is not None:
    print(f"{current}\t{count}")
```

本地调试：

```bash
cat input.txt | python mapper.py | sort | python reducer.py
```

提交到 Hadoop：

```bash
hadoop jar hadoop-streaming-*.jar \
  -input /input \
  -output /output \
  -mapper mapper.py \
  -reducer reducer.py \
  -file mapper.py \
  -file reducer.py
```

Streaming 的实现原理：

```text
Java wrapper 启动子进程
  ↓
通过 stdin 把输入喂给脚本
  ↓
读取脚本 stdout 作为 Map/Reduce 输出
```

核心价值：**让 Python、Shell、C++ 等非 Java 程序也能复用 MapReduce 的分布式运行时。**

---

## 17. 工程工具：多输入、多输出、DistributedCache

### MultipleInputs

一个作业读取多个路径、多个格式、多个 Mapper：

```java
MultipleInputs.addInputPath(job, new Path("/path/A"),
    TextInputFormat.class, MapperA.class);

MultipleInputs.addInputPath(job, new Path("/path/B"),
    SequenceFileInputFormat.class, MapperB.class);
```

适合多源数据 join、混合格式处理。

### MultipleOutputs

一个 Reducer 根据数据内容写到不同目录：

```text
Reducer 输出
  ├── 偶数 key → /output/even/
  └── 奇数 key → /output/odd/
```

适合按类型、日期、业务线拆分输出，避免写多个作业。

### DistributedCache

把小文件或依赖分发到每个节点本地，避免每个 Task 都远程读 HDFS。

| 类型 | 典型用途 |
|------|----------|
| 普通文件 | 黑名单、白名单、配置 |
| jar 包 | 第三方依赖 |
| 压缩包 | 词典、模型、多文件目录 |

对比：

```text
没有 DistributedCache：100 个 Task 各自读 HDFS，小文件被读 100 次
有 DistributedCache：作业启动时分发一次，Task 本地读取
```

---

## 18. 典型案例一：倒排索引

倒排索引是搜索引擎的核心结构：从“文档 → 单词”变成“单词 → 文档列表”。

输入：

```text
doc0: I wish to wish
doc1: you wish
doc2: I wish you
```

目标：

```text
I    → doc0, doc2
wish → doc0, doc1, doc2
you  → doc1, doc2
```

MapReduce 思路：

| 阶段 | 输出 |
|------|------|
| Mapper | `<word:doc, 1>` |
| Combiner | `<word, doc:count>` |
| Reducer | `<word, doc1:count,doc2:count,...>` |

Combiner 的价值：先在 Map 端统计某个词在某个文档中的次数，减少传输到 Reducer 的记录数。

```text
没有 Combiner：
<wish:doc0, 1>, <wish:doc0, 1>, <wish:doc0, 1>

有 Combiner：
<wish, doc0:3>
```

---

## 19. 典型案例二：利用排序做 COUNT(DISTINCT)

需求：

```sql
SELECT dealid, COUNT(DISTINCT uid)
FROM order
GROUP BY dealid;
```

如果 Reducer 里用 HashSet 保存所有 uid，数据很大时可能 OOM。MapReduce 更适合利用排序机制去重。

思路：

```text
Mapper 输出：<dealid+uid, 1>
Partitioner：按 dealid 分区，保证同一 dealid 到同一个 Reducer
Sort：框架按 dealid+uid 排序，重复 uid 会相邻
Reducer：只比较当前 key 和上一个 key，uid 变化才计数
```

Reducer 看到的数据：

```text
00001+12054
00001+12054  ← 重复，不计
00001+13000  ← uid 变化，计数 +1
00002+12090  ← dealid 变化，输出 00001 的结果，重置计数
```

两种方案对比：

| 方案 | 做法 | 内存 |
|------|------|------|
| HashSet 去重 | 保存所有 uid | uid 越多内存越大 |
| 排序去重 | 只记上一个 key | 常数内存 |

本质：**用 Shuffle + Sort 替代内存 HashSet。**

---

## 20. MapReduce 适合什么，不适合什么

MapReduce 适合“可以拆成独立子问题，最后再汇总”的任务。

适合：

| 任务 | 原因 |
|------|------|
| WordCount | 每条记录独立计数，最后按 key 汇总 |
| 日志统计 | 每条日志可独立处理 |
| 倒排索引 | 每篇文档可独立抽词，最后按词汇总 |
| Top K | 局部 Top K 后再求全局 Top K |
| ETL | 每条记录可独立清洗转换 |

不适合：

| 任务 | 原因 |
|------|------|
| 低延迟查询 | MapReduce 启动作业和 Shuffle 成本高 |
| 强迭代算法 | 每轮都要落盘，效率低 |
| Fibonacci 这类依赖链 | 后一步依赖前一步，无法充分并行 |
| 复杂图计算 | 点之间依赖强，反复迭代通信 |

一句话：**MapReduce 适合离线、高吞吐、可分治的数据并行任务；不适合低延迟、强交互、强迭代任务。**

---

## 21. 最终总结

MapReduce 的因果主线：

```text
海量数据无法单机处理
  ↓
需要把数据切成很多 split 并行处理
  ↓
用户只写 map() 处理单条记录
  ↓
相同 key 的中间结果必须汇聚到一起
  ↓
框架通过 Partitioner + Shuffle + Sort 完成分发、排序和分组
  ↓
用户只写 reduce() 聚合同 key 的 values
  ↓
运行时负责调度、容错、数据本地性、推测执行和输出
```

核心结论：

| 问题 | MapReduce 的回答 |
|------|------------------|
| 大文件怎么并行处理 | InputFormat 切 split，一个 split 一个 Map Task |
| 用户写什么 | Mapper 和 Reducer |
| 相同 key 怎么聚到一起 | Partitioner + Shuffle + Sort |
| 中间数据太多怎么办 | Spill、Merge、Combiner、压缩 |
| 机器挂了怎么办 | Task 失败重试 |
| 慢任务拖后腿怎么办 | 推测执行 |
| 如何减少网络读数据 | 数据本地性调度 |
| 非 Java 怎么用 | Hadoop Streaming |

一句话收束：**MapReduce 是一个把离线批处理固化为 Map → Shuffle → Reduce 的分布式运行时；它牺牲低延迟，换来易编程、可扩展、高吞吐和高容错。**