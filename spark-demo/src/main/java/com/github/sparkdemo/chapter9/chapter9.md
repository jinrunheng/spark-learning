## 第 9 章 Spark SQL

Spark SQL 是一种用来操作结构化和半结构化数据的接口。

结构化数据是指任何有结构信息的数据，而所谓的结构信息，就是每条记录公用的已知的字段集合。

Spark SQL 提供了以下三大功能：

1. Spark SQL 可以从各种结构化数据源（例如 JSON，Hive，Parquet 等）中读取数据
2. Spark SQL 不进支持在 Spark 程序内使用 SQL 语句进行数据查询，也支持从类似商业智能软件 Tableau 这样的外部工具中通过标准数据库连接器（JDBC/ODBC）连接 Spark SQL 进行查询
3. 当在 Spark 程序内使用 Spark SQL 时，Spark SQL 支持 SQL 与常规的 Java 代码高度整合，包括连接 RDD 与 SQL 表，公开的自定义 SQL 函数接口等。这样一来，许多工作都变得更容易实现



为了实现这些功能，Spark SQL 提供了一种特殊的 RDD，叫做 SchemaRDD。SchemaRDD 是存放 Row 对象的 RDD，每个 Row 对象代表一行记录。SchemaRDD 还包含记录的结构信息（即数据字段）。SchemaRDD 看起来和普通的 RDD 很像，但是在内部，SchemaRDD 可以利用结构信息更加高效地存储数据。此外，SchemaRDD 还支持 RDD 上没有的一些新操作，比如运行 SQL 查询等。SchemaRDD 可以从外部数据源创建，也可以从查询结果或普通 RDD 中创建。

### 9.1 连接 Spark SQL

我们需要添加 Spark Hive 的 Maven 依赖，来提供对 Hive 的支持：

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-hive_2.11</artifactId>
    <version>${spark.version}</version>
</dependency>
```

在这里推荐使用 HiveQL 作为 Spark SQL 的查询语言。

### 9.2 在应用中使用 Spark SQL

Spark SQL 最强大之处就是可以在 Spark 应用内使用。这让我们可以轻松读取数据并使用 SQL 查询，同时还能将这一过程和 Java 代码整合在一起。

要以这种方式使用 Spark SQL，需要基于已有的 SparkContext 创建出一个 HiveContext，如果使用的是去除了 Hive 支持的 Spark 版本，则创建出的是 SQLContext。这个上下文环境提供了对 Spark SQL 对数据进行查询和交互的额外函数。使用 HiveContext 可以创建出表示结构化数据的 SchemaRDD，并且使用 SQL 或者是类似 `map()` 的普通 RDD 操作来操作这些 SchemaRDD。

#### 9.2.1 初始化 Spark SQL

示例：在 Java 中创建 SQL 上下文环境

```java
JavaSparkContext ctx = new JavaSparkContext(...);
SQLContext sqlCtx = new HiveContext(ctx);
```

在有了 HiveContext 或者 SQLContext 之后，我们便可以准备读取数据并进行查询了。

#### 9.2.2 基本查询示例

要在一张数据表上进行查询，需要调用 HiveContext 或 SQLContext 的 `sql()` 方法。

我们需要先从 JSON 文件中读取一些推特数据，并将这些数据注册为一张临时表并赋予该表一个名字，然后就可以使用 SQL 来查询了。

接下来，就可以根据 `retweetCount` 字段（转发计数）选出最热门的推文：



