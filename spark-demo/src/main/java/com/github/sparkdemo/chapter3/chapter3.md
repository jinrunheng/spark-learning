## 第 3 章 RDD 编程
### 3.1 RDD 基础
RDD 即：弹性分布式数据集（Resilient Distributed Dataset）。在 Spark 中，对数据的所有操作不外乎创建为 RDD、转化已有 RDD 以及调用 RDD 操作进行求值。而在这背后，Spark 会自动将 RDD 中的数据分发到集群上，并将操作并行化执行。

用户可以使用两种方法来创建 RDD。第一种方法是通过读取一个外部的数据集，第二种方法是在驱动器程序里分发驱动器程序中的对象集合（比如 List 或 Set）。

譬如在 Java 中使用 `textFile()` 创建一个字符串的 RDD：

```java
JavaRDD<String> lines = sc.textFile(filePath);
```

创建后，RDD 支持两种类型的操作：**转化操作** 和 **行动操作**。

转化操作由一个 RDD 生成一个新的 RDD，而行动操作则是对 RDD 计算出一个结果，并把结果返回到驱动器程序中，或把结果存储到外部存储系统（如 HDFS）中。

转化操作和行动操作的区别在于 Spark 计算 RDD 的方式不同。Spark 采用**惰性求值**的机制，这些 RDD 只有第一次在一个行动操作中用到时，才会真正计算。

默认情况，Spark 的 RDD 会在你每次对它们进行行动操作时重新计算。如果想在多个行动操作中重用同一个 RDD，可以使用 `RDD.persist()` 让 Spark 将这个 RDD 缓存起来。

### 3.2 创建 RDD

我们讲过了 Spark 提供了两种方式来创建 RDD，第一种是通过读取一个外部的数据集：

```java
JavaRDD<String> lines = sc.textFile(filePath);
```

第二种也是最简单的方式，就是将程序中一个已有的集合传给 SparkContext 的 `parallelize()` 方法：

```java
JavaRDD<String> lines = sc.parallelize(Arrays.asList("pandas","i like pandas"));
```

不过这种方式除了开发原型和测试，用得并不多。更常用的方式是从外部存储中读取数据来创建 RDD。

### 3.3 RDD 操作

RDD 支持两种操作，**转化操作** 和 **行动操作**。

RDD 的转化操作是返回一个新的 RDD。譬如：`map()` 和 `filter()`。而行动操作则是向驱动程序返回结果或把结果写入外部系统的操作，会触发实际的计算，譬如：`count()` 和 `first()`。

#### 3.3.1 转化操作

举个例子：假定我们有一个日志文件 log.txt，内部有若干消息，如果我们要挑选出所有错误（error）和警告（warning），我们便可以这样做：

```java
public static void filter(String logPath) {
    // ...
    JavaRDD<String> inputRDD = sc.textFile(logPath);
    JavaRDD<String> errorRDD = inputRDD.filter(new Function<String, Boolean>() {
        @Override
        public Boolean call(String s) throws Exception {
            return s.contains("error");
        }
    });
    JavaRDD<String> warningRDD = inputRDD.filter(new Function<String, Boolean>() {
        @Override
        public Boolean call(String s) throws Exception {
            return s.contains("warning");
        }
    });
    JavaRDD<String> badLinesRDD = errorRDD.union(warningRDD);
}
```

通过转化操作，我们可以从已有的 RDD 中派生出新的 RDD，Spark 会使用**谱系图（lineage graph）** 来记录这些不同 RDD 之间的依赖关系：

![image-20211128115711362](https://tva1.sinaimg.cn/large/008i3skNgy1gwuq7riw1kj30s20ismxv.jpg)

#### 3.3.2 行动操作

继续使用前面的示例，我们如果想要输出关于 badLinesRDD 的一些信息，可以使用行动操作来实现。行动操作是对数据集进行实际的计算，将最终求得的结果返回到驱动器程序，或写入到外部存储系统中。

譬如，我们想要计算出 badLinesRDD 的计数结果：

```java
System.out.println("Input had " + badLinesRDD.count() + " concerning lines");
```

可以使用 `count()` 来实现。

我们还想收集 RDD 中的一些元素，可以使用 `take()` 来实现：

```java
for (String badLine : badLinesRDD.take(10)) {
    System.out.println(badLine);
}
```

`take()` 用于获取 RDD 中的少量元素。除此之外，RDD 还有一个 `collect()` 函数，用来获取完整的 RDD 中的数据，如果你的程序可以将 RDD 筛选到一个很小的规模，并且你想在本地处理它时，就可以使用。不过 `collect()` 不能使用到大规模的数据集上，如果数据规模很大，并且使用了 `collect()` 函数，很有可能会撑爆单机内存。所以，我们一般会将数据写入到譬如：HDFS 这样的分布式存储系统中。

#### 3.3.3 惰性求值

RDD 的转化操作都是**惰性求值**的，这也就意味着在被调用行动操作之前，Spark 并不会开始计算。

### 3.4 向 Spark 传递函数

在 Java 中，函数需要作为实现了 Spark 的 org.apache.spark.api.java.function 包中的任一函数接口的对象来传递。

我们可以使用匿名内部类这种方式：

```java
JavaRDD<String> errorRDD = inputRDD.filter(new Function<String, Boolean>() {
    @Override
    public Boolean call(String s) throws Exception {
        return s.contains("error");
    }
});
```

也可以使用实现了函数接口的具名类的方式：

```java
class ContainsError implements Function<String,Boolean>(){
  @Override
  public Boolean call(String s){return s.contains("error")}
}

JavaRDD<String> errorRDD = inputRDD.filter(new ContainsError());
```

还可以直接使用 Java 8 的 Lambda 表达式：

```java
JavaRDD<String> errorRDD = inputRDD.filter((Function<String, Boolean>) s -> s.contains("error"));
```

### 3.5 常用的转化操作和行动操作

#### 3.5.1 基本 RDD

##### 1. 针对各个元素的转化操作

最常用的转化操作是 `map()` 和 `filter()` 。

![image-20211128121829077](https://tva1.sinaimg.cn/large/008i3skNgy1gwuqtveo0ij30lg0ckq3h.jpg)

我们来看一个 `map()` 函数的示例：计算 RDD 中各个值的平方：

```java
JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
javaRDD.map((Function<Integer, Object>) x -> x * x);
```

有时候，我们希望对每个输入元素生成多个输出元素。实现该功能的操作叫做 `flatMap()`。和 `map()` 类似，我们提供给 `flatMap()` 的函数被分别应用到了输入 RDD 的每个元素上，不过其结果返回的不是一个元素，而是一个返回值序列的迭代器。

`flatMap()` 的一个简单用途是将输入的字符串切分为单词：

```java
JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hello spark"));
JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
words.collect().forEach(System.out::println);
```

该程序运行后，输出的结果为：

```text
hello
world
hello
spark
```

##### 2. 伪集合操作

RDD 本身不是严格意义上的集合，不过也支持许多数学上的集合操作，譬如合并与相交操作。

譬如：`distinct()`，`union()`，`intersection()`，`subtract()` 等等。

![image-20211128124447833](https://tva1.sinaimg.cn/large/008i3skNgy1gwurl8xi91j30y00jaac6.jpg)



`distinct()` 为去重操作，`union()` 为求两个集合所有的元素，`intersection()` 返回两个 RDD 中共有的元素，`subtract()` 返回一个只存在于第一个 RDD 中而不存在于第二个 RDD 中的所有元素组成的 RDD。

##### 3. 行动操作

基本 RDD 最常见的行动操作为 `reduce()`。它接收一个函数作为参数，这个函数要操作两个 RDD 的元素类型的数据并返回一个同样类型的新元素。

比较常见的操作是计算统计 RDD 中所有元素的总和：

```java
JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hello spark"));
JavaRDD<String> words = lines.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
JavaPairRDD<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1));
JavaPairRDD<String, Integer> wordCount = pairs.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);
```

RDD 的一些行动操作会以普通集合或者是值的形式将 RDD 的部分或全部数据返回驱动器程序中。

最常见的操作便是 `collect()`，它会将整个 RDD 的内容返回。我们之前说过，`collect()` 通常会在单元测试中使用，数据集不是很大的情况下，它可以放在单机的内存中。由于需要将数据复制到驱动器进程中，`collect()` 要求所有数据都必须能一同放入单台机器的内存中。

`take(n)` 返回 RDD 中的 n 个元素，并且尝试只访问尽量少的分区，因此该操作会得到一个不均衡的集合。需要注意的是这些操作返回元素的顺序和你预期的可能不太一样。

`top()` 用于从 RDD 中获取前几个元素，`top()` 函数使得数据的返回采用默认顺序。

`foreach()` 行动操作用来对 RDD 每个元素进行操作。

`count()` 用来返回元素的个数，`countByValue()` 则返回一个从各值到值对应的技术的映射表。

#### 3.5.2 在不同 RDD 类型间转换

Java 中有两个专门的类 ： JavaDoubleRDD 和 JavaPairRDD 用来处理特殊类型的 RDD。

要构建出这些特殊类型的 RDD，需要使用特殊版本的类来替代一般使用的 Function 类。譬如要从 T 类型的 RDD 创建出一个 DoubleRDD，我们就应该在映射操作中使用 DoubleFunction<T> 来替代 Function<T,Double>。

Java 中针对专门类型的函数接口：

| 函数名                       | 等价函数                          | 用途                                    |
| ---------------------------- | --------------------------------- | --------------------------------------- |
| DoubleFlatMapFunction<T>     | Function<T,Iterable<Double>>      | 用􏰩 flatMapToDouble，􏰚 生成DoubleRDD    |
| DoubleFunction<T>            | Function<T,Double>                | 用􏰩 mapToDouble，􏰚生成 DoubleRDD        |
| PairFlatMapFunction<T, K, V> | Function<T,Iterable<Tuple2<K,V>>> | 用􏰩 flatMapToPair，􏰚生 成 PairRDD<K, V> |
| PairFunction<T, K, V>        | Function<T,Tuple2<K,V>>           | 用􏰩mapToPair，􏰚生成 PairRDD<K, V>       |

示例：计算 RDD 中每个元素的平方值，并返回平均值

```java
JavaRDD<Integer> javaRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
javaRDD.mapToDouble((DoubleFunction<Integer>) x -> (double) x * x);
// 求出平均值
System.out.println(result.mean());
```

该程序输出的结果为：

```text
7.5
```

### 3.6  持久化（缓存）

Spark RDD 采用了惰性求值的机制，为了避免多次计算同一个 RDD，我们可以选择让 Spark 对数据进行持久化。

当我们让 Spark 持久化存储一个 RDD 时，计算出 RDD 的节点会分别保存它们所求出的分区数据。如果一个有持久化数据的节点发生故障，Spark 会在需要用到缓存的数据时重算丢失的数据分区。如果希望节点故障的情况下不会拖累我们的执行速度，也可以将数据备份到多个节点上。

在 Java 中，默认情况下，`persist()` 函数会将数据以序列化的形式缓存在 JVM 的堆空间中。

持久化的级别有以下几种：

| StorageLevel        | 使用的空间 | CPU 时间 | 是否在内存中 | 是否在磁盘上 | 备注                               |
| ------------------- | ---------- | -------- | ------------ | ------------ | ---------------------------------- |
| MEMORY_ONLY         | 高         | 低       | 是           | 否           |                                    |
| MEMORY_ONLY_SER     | 低         | 高       | 是           | 否           |                                    |
| MEMORY_AND_DISK     | 高         | 中等     | 部分         | 部分         | 如果内存中放不下，则溢写到磁盘上   |
| MEMORY_AND_DISK_SER | 低         | 高       | 部分         | 部分         | 􏷸􏰧如果内存中放不下，则溢写到磁盘上 |
| DISK_ONLY           | 低         | 高       | 否           | 是           |                                    |

示例程序：

```java
public static void testPersist() {
    JavaSparkContext sc = connectToSpark();
    JavaRDD<Integer> inputRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4));
    JavaRDD<Integer> squareRDD = inputRDD.map((Function<Integer, Integer>) x -> x * x);
    // 设置持久化级别为 DISK_ONLY 
  	squareRDD.persist(StorageLevel.DISK_ONLY());

    System.out.println(squareRDD.count());
    squareRDD.collect().forEach(System.out::println);
}
```

最后，RDD 还有一个方法叫 `unpersist()`，调用该方法可以手动地把持久化 RDD 从缓存中移除。

