# Spark-Learing

## 1. Spark 概述

### 1.1 Spark 是什么

Spark 是一种基于内存的快速、通用、可扩展的大数据分析计算引擎。

### 1.2 Spark and Hadoop

**Hadoop**

- 2006 年 1 月，Doug Cutting 加入 Yahoo，领导 Hadoop 的开发
- 2008 年 1 月，Hadoop 成为 Apache 顶级项目
- 2011 年，Hadoop 1.0 正式发布
- 2012 年 3 月，Hadoop 稳定版发布
- 2013 年 10 月，Hadoop 发布 2.X（Yarn）版本

**Spark**

- 2009 年，Spark 诞生于伯克利大学的 AMPLab 实验室
- 2010 年，伯克利大学正式开源 Spark 项目
- 2013 年 6 月，Spark 成为了 Apache 下的项目
- 2014 年 2 月，Spark 成为了 Apache 顶级项目
- 2015 年至今，Spark 变得愈发火爆，大量的国内公司开始重点部署或使用 Spark

从功能上看：

**Hadoop**

- Hadoop 是由 Java 编写，在分布式服务器集群上存储海量数据并运行分布式分析应用的开源框架。
- 作为 Hadoop 分布式文件系统，HDFS 处于 Hadoop 生态圈的最下层，存储着所有的数据，支持着 Hadoop 所有服务，它的理论基础源于 Google 的论文 《The Google File System》，它是 GFS 的开源实现。
- MapReduce 是一种编程模型，Hadoop 根据 Google 的 MapReduce 论文将其实现，作为 Hadoop 的分布式计算模型，是 Hadoop 的核心。基于这个框架，分布式并行程序的编写变得非常简单。综合了 HDFS 的分布式存储和 MapReduce 的分布式计算，Hadoop 在处理海量数据时，性能横向扩展变得非常容易。
- HBase 是对 Google 的 Bigtable 的开源实现，但又和 Bigtable 存在许多不同之处。HBase 是一个基于 HDFS 的分布式数据库，擅长实时地随机读/写超大规模数据集。它也是 Hadoop 非常重要的组件。

**Spark**

- Spark 是一种由 Scala 语言开发的，快速、通用、可扩展的大数据分析引擎。
- Spark Core 中提供了 Spark 最基础与最核心的功能。
- Spark SQL 是 Spark 用来操作结构化数据的组件。通过 Spark SQL，用户可以使用 SQL 或者 Apache Hive 版本的 SQL 方言（HQL）来查询数据。
- Spark Streaming 是 Spark 平台上针对实时数据进行流式计算的组件，提供了丰富的处理数据流的 API。



由上述分析可知，Spark 出现的时间相对较晚，并且主要功能是用于数据计算。所以，Spark 一直被认为是 Hadoop 框架的升级版。

### 1.3 Spark vs Hadoop

Hadoop 的 MR 框架与 Spark 框架都是数据处理框架，那么我们在使用时该如何选择呢？

- Hadoop 的 MapReduce 由于其设计初衷并不是为了满足循环迭代式数据流处理，因此在多并行运行的数据可复用场景中会浪费磁盘 I/O，存在计算效率问题。所以，Spark 应运而生。Spark 就是在传统的 MapReduce 计算框架的基础上，利用其计算过程的优化，从而大大加快了数据分析的运行和读写速度，并将计算单元缩小到更适合并行计算和重复使用的 RDD 计算模型。
- Spark 是一个分布式数据快速分析项目。它的核心技术是弹性分布式数据集（Resilient Distributed Datasets），提供了比 MapReduce 丰富的模型，可以快速在内存中对数据集进行多次迭代，来支持复杂的数据挖掘算法和图形计算算法。
- Spark 和 Hadoop 的根本差异是多个作业之间的数据通信问题。Spark 多个作业之间数据通信是基于内存，而 Hadoop 则是基于磁盘。
- Spark Task 启动时间快。Spark 采用 fork 线程的方式，而 Hadoop 采用创建新的进程的方式。
- Spark 只有在 shuffle 的时候将数据写入磁盘，而 Hadoop 中多个 MR 作业之间的数据交互都要依赖于磁盘交互。
- Spark 的缓存机制比 HDFS 的缓存机制高效。



经过上面的比较，我们可以看出在绝大多书的数据计算场景中，Spark 确实会比 MapReduce 更有优势。但是 Spark 是基于内存的，所以在实际的生产环境中，由于内存的限制，可能会由于内存资源不够导致 Job 执行失败。此时，MapReduce 其实是一个更好的选择，所以，Spark 并不能完全替代 Hadoop MR。

### 1.4 Spark 核心模块

- Spark Core

  Spark Core 中提供了 Spark 最基础与最核心的功能，Spark 其他的功能如：Spark SQL，Spark Streaming，GraphX，MLlib 都是在 Spark Core 的基础上进行扩展的。

- Spark SQL

  Spark SQL 是 Spark 用来操作结构化数据的组件。通过 Spark SQL，用户可以使用 SQL 或者 Apache Hive 版本的 SQL 方言（HQL）来查询数据。

- Spark Streaming

  Spark Streaming 是 Spark 平台上针对实时数据进行流式计算的组件，提供了丰富的处理数据流的 API。

- Spark MLlib

  MLlib 是 Spark 提供的一个机器学习算法库。MLlib 不仅提供了模型评估，数据导入等额外功能，还提供了一些更底层的机器学习原语。

- Spark GraphX

  GraphX 是 Spark 面向图计算提供的框架与算法库。

## 2. Spark 快速上手

### 2.1 Word Count

![image-20211127112807879](https://tva1.sinaimg.cn/large/008i3skNgy1gwtjr7genmj32040pu42d.jpg)

Java 代码：

```java
package com.github.sparkdemo.lesson2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

    public static void main(String[] args) {

        // 建立与 Spark 的连接
        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 1. 读取文件，获取一行一行的数据
        String filePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/hello.txt";
        JavaRDD<String> lines = sc.textFile(filePath);

        // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
        // hello word / hello spark => hello,word,hello,spark
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // 3. 将数据根据单词进行分组，便于统计（转换为 <word,1> 的格式）
        // (hello,hello),(world),(spark)
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 4. 对分组后的数据进行聚合，统计相同 word 出现的频率
        // (hello,2),(world,1),(spark,1)
        JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 5. 执行 action 将结果打印出来
        wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1() + " " + t._2());
            }
        });

        // 6. 将结果输出到一个目录中
        String outputFilePath = "/Users/macbook/Desktop/myProject/spark-learing/spark-demo/src/main/resources/output";
        wordCount.saveAsTextFile(outputFilePath);

        // 关闭 SparkContext
        sc.stop();
    }
}
```

## 3. Spark 运行环境

Spark 作为一个数据处理框架和计算引擎，被设计在所有常见的集群环境中运行，在国内工作中主流的环境为 Yarn，不过容器式环境也渐渐流行起来了。接下来，我们就分别看看不同环境下 Spark 的运行。

## 3.1 Local 模式

所谓的 Local 模式，就是不需要其他任何节点的资源便可以在本地执行 Spark 代码的环境。

我们可以开启一个 Linux 虚拟机，也可以使用 Docker 容器来搭建环境，我使用的是后者的方式。

首先，基于 CentOS 8.4 的镜像创建一个 Linux 服务器环境，命令如下：

```bash
docker run -it --name hadoop-basenv -v /Users/macbook/Desktop/data/docker/data:/home/data centos:6.6 /bin/bash
```

接下来，我们需要在创建好的容器中安装 Spark 相关基础环境。

**安装 Vim**

执行命令：

```bash
yum install vim-enhanced
```

**安装 JDK**

首先将 JDK 官网的 rpm 安装包 `jdk-8u202-linux-x64.rpm` 放到本地目录 `/Users/macbook/Desktop/data/docker/data` 下，因为创建容器时，我们将此目录挂载到容器中的 `/home/data` 下，所以我们可以在 `/home/data` 目录下找到 JDK 安装包，通过这种挂载的方式我们可以在宿主机和容器之间共享数据。执行命令，安装 JDK：

```bash
rpm -ivh jdk-8u202-linux-x64.rpm
```

**安装 Spark**

在官网下载 Spark 安装包，解压后放到本地共享目录下，我使用的是 `spark-3.2.0-bin-hadoop.3.2`。将文件夹名修改为 `spark-local`。

**配置环境变量**

```bash
SPARK_HOME=/home/data/spark-local/
JAVA_HOME=/usr/java/jdk1.8.0_202-amd64/
JRE_HOME=/usr/java/jdk1.8.0_202-amd64/jre

CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin

export JAVA_HOME JRE_HOME CLASSPATH SPARK_HOME PATH
```

上述操作全部执行完毕后，进入到 `spark-local` 目录下执行命令：

```bash
bin/spark-shell
```

![image-20211127153121295](https://tva1.sinaimg.cn/large/008i3skNgy1gwtqsdidp7j31nf0u0n7y.jpg)

当出现 Spark 的 Logo，并且开启了 Scala 命令行时，说明我们的本地 Local 环境配置成功。

在 Local 环境下，如何提交我们的应用呢？

一般我们会将开发的 Java 程序打成 Jar 包，然后使用 `spark-submit` 命令来进行提交：

```bash
bin/spark-submit \
--class org.apache.spark.examples.SparkPi \
--master local[2] \
./examples/jars/spark-examples_2.12-3.2.0.jar \
10
```

上面的命令中，`SparkPi` 是官方提供给我们的一个示例程序，用来计算圆周率。 

- `--class` 表示要执行程序的主类，此处可以更换为我们自己写的应用程序
- `--master local[2]` 部署模式，默认为本地模式，数字表示分配虚拟 CPU 核数量
- `spark-examples_2.21-3.2.0.jar` 为运行的应用类所在的 jar 包，实际使用时，可以设定为我们自己打的 jar 包
- 数字 10 表示程序的入口参数，用于设定当前应用的任务数量



Local 模式一般用于个人测试使用，真实的工作中需要将应用提交到对应的 Spark 集群中去执行。

## 4. Spark 运行架构

### 4.1 运行架构

Spark 框架的核心是一个计算引擎，整体来说，它采用了标准的 Master-Slave 结构。

如下图所示，它展示了一个 Spark 执行时的基本结构。图形中的 Driver 表示 Master，负责管理整个集群中的作业任务调度。图形中的 Executor 则是 Slave，负责实际执行任务。

![image-20211127164101156](https://tva1.sinaimg.cn/large/008i3skNgy1gwtsssevy8j318w0qmjt8.jpg)

### 4.2 核心组件

由上图可以看出，对于 Spark 框架有两个核心组件：

#### 4.2.1 Driver

Driver 为 Spark 驱动器节点，用户执行 Spark 任务中的 main 方法，负责实际代码的执行工作。

Driver 在 Spark 作业执行时主要负责：

- 将用户程序转化为作为（Job）
- 在 Executor 之间调度任务（task）
- 跟踪 Executor 的执行情况
- 通过 UI 展示查询运行情况

实际上，我们无法准确描述 Driver 的定义，因为在整个编程过程中，没有看到任何有关 Driver 的字眼。所以，简单理解，所谓的 Driver 就是驱使整个应用运行起来的程序，也称之为 Driver 类。

#### 4.2.2 Executor

Spark Executor 是集群中工作节点（Worker）中的一个 JVM 进程，负责 Spark 作业中运行具体任务（Task），任务之间彼此相互独立。Spark 应用启动时，Executor 节点被同时启动，并始终伴随着整个 Spark 应用的生命周期而存在。如果有 Executor 节点发生了故障或崩溃，Spark 应用也可以继续执行，会将出错节点上的任务调度到其他 Executor 节点上继续运行。

Executor 有两个核心功能：

- 负责运行组成 Spark 应用的任务，并将结果返回给驱动器进程
- 它们通过自身的块管理器（Block Manager）为用户程序中要缓存的 RDD 提供内存式存储。RDD 是直接缓存在 Executor 进程内的，因此任务可以在运行时充分利用缓存数据加速运算。

#### 4.2.3 Master & Worker

Spark 集群的独立部署环境中，不需要依赖其他的资源调度框架，自身就实现了资源调度的功能，所以环境中还有其他两个核心组件：Master 和 Worker。这里面的 Master 是一个进程，主要负责资源的调度和分配，并进行集群的监控等职责，类似于 Yarn 环境中的 RM，而 Worker 也是一个进程，一个 Worker 运行在集群中的一台服务器上，由 Master 分配资源对数据进行处理和计算，类似于 Yarn 中的 NM。

#### 4.2.4 ApplicationMaster

Hadoop 用户向 YARN 集群提交应用程序时，提交程序中应该包含 ApplicationMaster，用于向资源调度器申请执行任务的资源容器 Container，运行用户自己的程序任务 Job，监控整个任务的执行，跟踪整个任务的状态，处理任务失败等异常情况。

简单来说，ResourceManager（资源）和 Driver（计算）之间的解耦靠的就是 ApplicationMaster。

### 4.3 核心概念

#### 4.3.1 Executor 与 Core

Spark Executor 是集群中运行在工作节点（Worker）中的一个 JVM 进程，是整个集群中专门用于计算的节点。在提交应用中，可以提供参数指定计算节点的个数，以及对应的资源。这里的资源一般指的是工作节点 Executor 的内存大小和使用虚拟 CPU 核（Core）数量。

应用程序相关启动参数如下：

| 名称              | 说明                                   |
| ----------------- | -------------------------------------- |
| --num-executors   | 配置 Executor 的数量                   |
| --executor-memory | 配置每个 Executor 的内存大小           |
| --executor-cores  | 配置每个 Executor 的虚拟 CPU Core 数量 |

#### 4.3.2 并行度（Parallelism）

在分布式计算框架中一般都是多个任务同时执行，由于任务分布在不同的计算节点进行计算，所以能够真正地实现多任务并行执行，记住，这里是并行，而不是并发。我们将整个集群并行执行任务的数量称之为并行度。那么一个作业的并行度到底是多少呢？这个取决于框架的默认配置。应用程序也可以在运行过程中动态地修改。

#### 4.3.3 有向无环图（DAG）

![image-20211127172557214](https://tva1.sinaimg.cn/large/008i3skNgy1gwtu3i8bilj30n20k00t9.jpg)

大数据计算引擎框架中，我们一般会根据使用方式的不同分为四类，其中第一类便是 Hadoop 所承载的 MapReduce，它将计算分为两个阶段，分别是 Map 阶段和 Reduce 阶段。

对于上层应用来说，就不得不想方设法拆分算法，甚至于不得不在上层应用实现多个 Job 的串联，以完成一个完整的算法，例如迭代计算。由于这样的弊端，催生了支持 DAG 框架的产生。因此，。支持 DAG 框架被划分为第二代计算引擎。如 Tez 以及更上层的 Oozie。我们不去细究各种 DAG 实现之间的区别，不过对于当时的 Tez 和 Oozie 来说，大多数还是批处理的任务。接下来就是以 Spark 为代表的第三代的计算引擎。第三代计算引擎的特点主要是 Job 内部的 DAG 支持（不跨越 Job），以及实时计算。

这里所谓的有向无环图，并不是真正意义的图形，而是由 Spark 程序直接映射成的数据流的高级抽象模型。简单理解就是将整个程序计算的执行过程用图形表示出来，这样更直观，更便于理解，可以用于表示程序的拓扑结构。

### 4.4 提交流程

所谓的提交流程，其实就是开发人员根据需求写的应用程序通过 Spark 客户端提交给 Spark 运行环境执行计算的流程。在不同的部署环境中，这个提交过程基本相同，但是又有细微的区别，我们在这里不仅行详细的比较，但是因为在国内，将 Spark 引用部署到 Yarn 环境中会更多一些，所以我们这里的提交流程是基于 Yarn 环境的。

![image-20211127174812077](https://tva1.sinaimg.cn/large/008i3skNgy1gwtuqo7384j312o0u0tav.jpg)

Spark 应用程序提交到 Yarn 环境中执行的时候，一般会有两种部署执行的方式：Client 和 Cluster。两种模式主要区别在于：Driver 程序运行节点位置。

