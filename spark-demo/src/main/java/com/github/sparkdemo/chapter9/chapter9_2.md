## Spark SQL

Spark SQL 是 Spark 的一个模块，主要用来操作结构化和半结构化数据的接口。结构化数据是指任何有结构信息的数据。它的核心编程抽象为 **DataFrame**。

所谓的结构信息就是每条记录共用的已知字段集合。当数据符合这样的条件时，Spark SQL 就会使得针对这些数据的读取和查询变得更加简单高效。

Spark SQL 提供以下三大功能：

1. Spark SQL 可以从各种结构化数据源（例如 JSON，Hive，Parquet 等）中读取数据。
2. Spark SQL 不仅支持在 Spark 程序内使用 SQL 语句进行数据查询，也支持从类似商业智能软件 Tableau 这样的外部工具中通过标准数据库连接器（JDBC/ODBC）连接 Spark SQL 进行查询。
3. 当在 Spark 程序内使用 Spark SQL 时，Spark SQL 支持 SQL 与常规的 Java 代码高度整合，包括连接 RDD 与 SQL 表，公开自定义 SQL 函数接口等。这样一来，许多工作都变得更容易实现了。

### 一、SparkSession