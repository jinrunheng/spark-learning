## 第 4 章 健值对操作
键值对 RDD 通常用来进行聚合计算，它提供了一些新的操作接口，以便我们实现一些操作，譬如统计每个产品的评论，将数据中键相同的分为一组，将两个不同的 RDD 进行分组合并，等等。

### 4.1 动机

Spark 为包含键值对类型的 RDD 提供了一些专有的操作。这些 RDD 被称为 Pair RDD。Pair RDD 是很多程序的构成要素，因为它们提供了并行操作各个键或跨节点重新进行数据分组的操作接口。譬如：Pair RDD 提供了 `reduceByKey()` 方法，可以分别归约每个键对应的数据，还有 `join()` 方法，可以将两个 RDD 中键相同的元素组合到一起，合并为一个 RDD。我们通常从一个 RDD 中提取某些字段，譬如代表事件时间，用户 ID 或者其他标识符的字段，并使用这些字段为 Pair RDD 操作中的键。

### 4.2 创建 Pair RDD

示例：在 Java 中用使用第一个单词作为键创建出一个 Pair RDD

```java
public static void testMapToPair() {
    JavaSparkContext sc = connectToSpark();
    JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "hello spark", "hello wtf"));
        
    JavaPairRDD<String, String> pairRDD = lines.mapToPair(new PairFunction<String, String, String>() {
        @Override
        public Tuple2<String, String> call(String s) throws Exception {
            return new Tuple2<>(s.split(" ")[0], s);
        }
    });
  
    pairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
        @Override
        public void call(Tuple2<String, String> stringIntegerTuple2) throws Exception {
            System.out.println(stringIntegerTuple2._1() + " : " + stringIntegerTuple2._2());
        }
    });
}
```

运行程序，输出结果为：

```text
hello : hello world
hello : hello spark
hello : hello wtf
```

### 4.3 Pair RDD 转化操作

Pair RDD 可以使用所有标准 RDD 上的可用的转化操作。

由于 Pair RDD 中包含二元组。所以需要传递的函数应当操作二元组而不是独立的元素。



Pair RDD 的转化操作（以键值对集合 `{(1, 2), (3, 4), (3, 6)}` 为例）

| 函数名                                                       | 目的                                                         | 示例                               | 结果                                               |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ---------------------------------- | -------------------------------------------------- |
| reduceByKey(func)                                            | 合并具有相同键的值                                           | `rdd.reduceByKey((x, y) => x + y)` | `{(1, 2), (3,10}`                                  |
| groupByKey()                                                 | 对具有相同键的值进行分组                                     | `rdd.groupByKey()`                 | `{(1,[2]),(3,[4,6])}`                              |
| combineBy Key(createCombiner, mergeValue, mergeCombiners, partitioner) | 使用不同的返回类型合并具有相同键的值                         |                                    |                                                    |
| mapValues(func)                                              | 􏰃对 Pair RDD 中的每个值应用一个函数而不改变键                | `rdd.mapValues(x => x+1)`          | `{(1, 3), (3, 5), (3, 7)}`                         |
| flatMapValues(func)                                          | 对 Pair RDD 中的每个值应用一个返回迭代器的函数，然后对返回的每个元素都生成一个对应原键值对记录，通常用于符号化 | `rdd.flatMapValues(x => (x to 5))` | `{(1, 2), (1, 3), (1, 4), (1, 5), (3, 4), (3, 5)}` |
| keys()                                                       | 返回一个仅包含键的 RDD                                       | `rdd,keys()`                       | `{1, 3, 3}`                                        |
| values()                                                     | 返回一个仅包含值的 RDD                                       | `rdd.values()`                     | `{2, 4, 6}`                                        |
| sortByKey()                                                  | 返回一个根据键排序的 RDD                                     | `rdd.sortByKey()`                  | `{(1, 2), (3, 4), (3, 6)}`                         |



针对两个 Pair RDD 的转化操作（`rdd = {(1, 2), (3, 4), (3, 6)}`， `other = {(3, 9)}`）

| 函数名         | 目的                                       | 示例                        | 结果                                                 |
| -------------- | ------------------------------------------ | --------------------------- | ---------------------------------------------------- |
| subtractByKey  | 删掉 RDD 中键与 other RDD 中的键相同的元素 | `rdd.subtractByKey(other)`  | `{(1, 2)}`                                           |
| Join           | 对两个 RDD 进行内连接                      | `rdd.join(other)`           | `{(3, (4, 9)), (3, (6, 9))}`                         |
| rightOuterJoin | 对两个 RDD 进行右外连接                    | `rdd.rightOuterJoin(other)` | `{(3,(Some(4),9)), (3,(Some(6),9))}`                 |
| leftOuterJoin  | 对两个 RDD 进行左外连接                    | `rdd.leftOuterJoin(other)`  | `{(1,(2,None)), (3, (4,Some(9))), (3, (6,Some(9)))}` |
| cogroup        | 将两个 RDD 中拥有相同键对数据分组到一起    | `rdd.cogroup(other)`        | `{(1,([2],[])), (3, ([4, 6],[9]))}`                  |

示例：用 Java 对第二个元素进行筛选，筛选掉长度超过 20 的行

```java
Function<Tuple2<String, String>, Boolean> longWordFilter = new Function<Tuple2<String, String>, Boolean>() {
	public Boolean call(Tuple2<String, String> keyValue) { 
    return (keyValue._2().length() < 20);
  }  
};

JavaPairRDD<String, String> result = pairs.filter(longWordFilter);
```

该操作的示意图如下所示：

![image-20211128152019866](https://tva1.sinaimg.cn/large/008i3skNgy1gwuw39x11lj313u0c2gmg.jpg)

#### 4.3.1 聚合操作

Spark 有一些操作，可以组合具有相同键的值。这些操作返回 RDD，因此它们是转化操作而不是行动操作。

最常见的就是 `reduceByKey()`，该函数与 `reduce()` 类似，它们都接收一个函数，并使该函数对值进行合并。

示例：用 Java 实现单词计数

```java
public static void wordCount() {
    String textPath = "/.../hello.txt";
    JavaSparkContext sc = connectToSpark();
    JavaRDD<String> lines = sc.textFile(textPath);
    lines
            .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator())
            .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
            .reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b)
            .foreach((VoidFunction<Tuple2<String, Integer>>) t -> System.out.println(t._1() + " : " + t._2()));
}
```

或者可以直接对第一个 RDD 使用 `countByValue()` 函数来实现单词计数功能：

```java
public static void wordCount2() {
    String textPath = "/.../hello.txt";
    JavaSparkContext sc = connectToSpark();
    JavaRDD<String> lines = sc.textFile(textPath);
    Map<String, Long> map = lines
            .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator())
            .countByValue();
    map.forEach((K, V) -> System.out.println(K + " : " + V));
}
```

除了 `reduceByKey()` ，`combineByKey()` 也是最为常用的基与键进行聚合的函数。

要理解 `combineByKey()` 就要理解它在处理数据时是如何处理每个元素的。由于 `combineByKey()` 会遍历分区中的所有元素，因此每个元素的键要么还没有遇到过，要么就和之前的某个元素的键相同。

如果是一个新的元素，`combineByKey()` 会使用 `createCombiner()` 函数创建那个键对应的累加器的初始值。

如果是一个在处理当前分区之前已经过到过的键，它会使用 `mergeValue()` 方法将该键的累加器对应的当前值与这个新的值进行合并。

示例：在 Java 中使用 `combineByKey()` 求每个键对应的平均值

test.txt

```text
panda 0
pink 3
pirate 3
panda 1
pink 4
```

calculateKeyAvg

```java
public static class AvgCount implements Serializable {
    public int total_;
    public int num_;

    public AvgCount(int total, int num) {
        total_ = total;
        num_ = num;
    }

    public double avg() {
        return total_ / (double) num_;
    }

}

public static void calculateKeyAvg() {
    String textPath = "/.../test.txt";
    JavaSparkContext sc = connectToSpark();
    JavaRDD<String> lines = sc.textFile(textPath);
    JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
        }
    });

    Function<Integer, AvgCount> createAcc = (Function<Integer, AvgCount>) x -> new AvgCount(x, 1);
    Function2<AvgCount, Integer, AvgCount> addAndCount = (Function2<AvgCount, Integer, AvgCount>) (avg, x) -> {
        avg.total_ += x;
        avg.num_ += 1;
        return avg;
    };
  
    Function2<AvgCount, AvgCount, AvgCount> combine = (Function2<AvgCount, AvgCount, AvgCount>) (avg1, avg2) -> {
        avg1.total_ += avg2.total_;
        avg1.num_ += avg2.num_;
        return avg1;
    };
        
    JavaPairRDD<String, AvgCount> avgCount = pairRDD.combineByKey(createAcc, addAndCount, combine);
    Map<String, AvgCount> countMap = avgCount.collectAsMap();

    countMap.forEach((K, V) -> System.out.println(K + " : " + V.avg()));
}
```

该程序运行的结果为：

```text
pirate : 3.0
pink : 3.5
panda : 0.5
```

#### 4.3.2 数据分组

对于有键的数据，一个常见的用例是将数据根据键进行分组

