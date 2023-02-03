## 2A, mostly practical 
Suppose you have a dataset (the format is not important, it might be parquet, csv,
…) and it has two columns: ‘user_id’ and ‘score’. The column ‘user_id’ contains only distinct values. The column
‘score’ contains numerical values. (You can easily generate such a dataset)

### 2A1, 
Suggest a way using Spark, how to add a new column which will contain percentile rank of the score for each
user. 2A2, Assume that Spark is running in distributed environment and suggest a way how to compute the percentile
rank in parallel (in the first step you might experience that Spark is trying to collapse data into a single
partition - suggest a way how to avoid that) 2A3, Try to explain why we are trying to avoid single partition and
what are the consequences of having a single partition during the computation. (Think about how distributed
computation in Spark works). Use Spark with Python or Scala. For 2A1, 2A2 we want the actual code,
2A3 is a theoretical question.
```python
import random
from pyspark.sql import functions as F
from pyspark.sql import Window
# generate test dataset
data = []
for i in range(1, 10000):
    data.append({'user_id': i, 'score': random.randint(1, 100)})

df = spark.createDataFrame(data)
```
### 2A1
```python
df = df.withColumn("Percentile", F.percent_rank().over(Window.orderBy(F.col("score"))))
```
### 2A2

We would have to add some column to partition the dataframe to be able to parallelize the computation.
Such column could be for example 'group_id' but percentile would be computed for each group separately so it
kind of defeats the purpose of percentile_ranking.
```python
df = df.withColumn("Percentile", F.percent_rank().over(Window.partitionBy("group_id").orderBy(F.col("score"))))
```
### 2A3

Single partition generates single task. Which means especially with huge datasets that there is no parallelization
which can result in memory spill or even out of memory errors. If the partition is too large to fit in the RAM of
the executor it writes surplus data into disk. This can significantly slow down the computation time.

## 2B Caching

Assume we have a parquet file with these three columns: col1, col2, col3 (all have numerical values). Next we
create a Spark DataFrame as follows:
```python
df = spark.read.parquet(path_to_the_data)
```
In the next step we filter the data and use caching on the filtered DataFrame:
```python
df.select('col1', 'col2').filter(col('col2') > 100).cache()
df.count()
```
Now we run these three queries:
```python
df.select('col1', 'col2').filter(col('col2') > 101).collect()
df.select('col1', 'col2').withColumn('col4', lit('test')).filter(col('col2') > 100).collect()
df.select('col1').filter(col('col2') > 100).collect()
```
Which of these three queries will take the data from cache? Please explain your answer.

Only the third query uses cached memory as the analyzed logical plan is the same as for the cached query.
First query contains different filter resulting in different analyzed logical plan.
Second query contains transformation on additional column which also results in different analyzed logical plan.

## 2C, theoretical

Partitioning and bucketing in Spark:

### 2C1, What is the purpose and how is partitioning and bucketing used in Spark? In this case we mean file system output partitioning.

Partitioning and bucketing are techniques used in Spark to optimize the storage of large datasets.
Partitioning divides the data into smaller, manageable chunks called partitions, which can be
processed in parallel per task, resulting in faster processing time. Bucketing organizes data within partitions into
smaller, more optimized units called buckets, making it easier to retrieve and process subsets of data within a
partition.

### 2C2, What are the differences between them and what are the situation one or another is used (or both)?

Partitioning and bucketing have different purposes and trade-offs. Partitioning is used to distribute data across
nodes in a cluster and to improve query performance by reducing the amount of data that needs to be processed.
Each partition can be processed as separate task.
Bucketing is used to improve query performance by reducing the number of data blocks that need to be read and
processed for a given query. Partitioning is typically used for large datasets, while bucketing is used for both
large and small datasets, but is especially useful for small datasets where it can improve query performance.
Good bucket setup can lead to shuffle-free joins, shuffle-free aggregations, but also faster filter performance
on bucketed columns.

### 2C3, Are there any side effects one should be aware of in practical usage?

There are some side effects that one should be aware of in practical usage:

- Both too many partitions and too few partitions can result in slower performance.
- Over-bucketing can result in thousands of small files which will result in slower performance
- Partitioning and bucketing can affect data distribution and skew, potentially leading to
imbalanced processing and reduced performance. If one value of bucketed column has significantly more records
than others.
- Partitioning and bucketing must be carefully planned and managed,
taking into account factors such as data distribution and query patterns, to ensure optimal performance.
