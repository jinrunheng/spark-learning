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





