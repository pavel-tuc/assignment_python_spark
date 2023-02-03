# Home assignments (Pavel)

## 1, Python

### 1A, object store path min/max analysis
You have a specific prefix + key structure in your objects store (can be S3, HDFS, ...), that looks like this:
`protocol://bucket/base_path/specific_path/keys`  and a key has a structure of `id=some_value/month=yyyy-MM-dd/object{1, 2, 3, ...}`

Example:
s3://my-bucket/xxx/yyy/zzz/abc/id=123/month=2019-01-01/2019-01-19T10:31:18.818Z.gz
s3://my-bucket/xxx/yyy/zzz/abc/id=123/month=2019-02-01/2019-02-19T10:32:18.818Z.gz
s3://my-bucket/xxx/yyy/zzz/abc/id=333/month=2019-03-01/2019-06-19T10:33:18.818Z.gz
s3://my-bucket/xxx/yyy/zzz/def/id=123/month=2019-10-01/2019-10-19T10:34:18.818Z.gz
s3://my-bucket/xxx/yyy/zzz/def/id=333/month=2019-11-01/2019-12-19T10:35:18.818Z.gz

You have a function `get_all_keys(bucket, full_path) -> Iterator[str]` for getting all the keys for a full path (base_path + specific_path).

Notes:
On the input you know your bucket, base_path and all the specific paths you want to generate output for.
Also as shown in the example the month subkey has format of a date, but it's always yyyy-MM-01, so effectively it only gives you information about the year and month. Objects (files) within this structure have a timestamp, but this is a timestamp of when they have been created. For illustration, the last line in the example is an object (file) that was generated at '2019-12-19T10:35:18.818Z', but data in it are for the id of '333' and month of 2019-11.

**For each specific_path (there can be many):**
 - A, calculate for each id a minimum and maximum month (there cannot be gaps between moths)
 - B, write the output to a json file
 - C, (optional/bonus) there can be gaps between months (missing months), so report them also in some appropriate structure

### 1B, parallel upload
Your task is to upload data from an object store (can be S3, HDFS, ...) to Elastic.
Elastic server has a given number of data nodes e.x. 5 and each one of the nodes will hold some of the indices you want to upload your data into. You can get the info by an API call such as `http://your-elastic-server:port/_cat/shards` this will get you an output of all the indices and details about them in a structure: name_of_the_index, (some unimportant stats), data_node

Example:
index-2018-01 … data-node-01
index-2018-02 … data-node-03
index-2018-03 … data-node-02
index-2018-04 … data-node-04
index-2018-05 … data-node-04
index-2018-06 … data-node-05
index-2018-07 … data-node-01

As you can see the distribution is quite random, but it's going to be pretty even across the data-nodes.

One specific index will always hold data for a year and month combination e.x. index-2018-01 will have all the data for 2018-01. Luckily your teammates already prepared the data for you with this structure in mind, so your data in object store are partitioned by year and month as you need, e.x. `created_year=2017/created_month=1`, `created_year=2018/created_month=12`, etc. and you also have a function `write_to_elastic_index(df, year, month, target_index)` you can leverage.
All you have to do is point our df (dataframe) to the right location, specify the partition filters (year and month) and target_index and it will do the dirty work for you (well, in reality it's going to leverage the elasticsearch-spark library).

```python
def write_to_elastic_index(df, year, month, target_index) -> None:
    """Writes data filtered from dataframe (df) by the created_year=/created_month partition filter into given Elastic index.

    Example:
    # write data from object store partition 'created_year=2018/created_month=1' into Elasticsearch index named 'index-2018-01'
    write_to_elastic(df, 2018, 1, 'index-2018-01')
"""

```

To effectively utilize the resources, it makes sense to parallelize the task as much as possible, but you cannot process more than one write request per data-node, otherwise it will crush. Write an application that will handle uploading all the data in the most effective way.
You can assume a fixed number of data-nodes (or find out the number based on the API response) and data at least from `2017-01 (created_year=2017/created_month=1)` to `2020-01 (created_year=2020/created_month=1)` without any gaps.


## 2, Spark (solve with Spark 2.4+)

### 2A, mostly practical

Suppose you have a dataset (the format is not important, it might be parquet, csv, ...) and it has two columns: ‘user_id’ and ‘score’. The column ‘user_id’ contains only distinct values. The column ‘score’ contains numerical values. (You can easily generate such a dataset)

- 2A1, Suggest a way using Spark, how to add a new column which will contain percentile rank of the score for each user.
- 2A2, Assume that Spark is running in distributed environment and suggest a way how to compute the percentile rank in parallel (in the first step you might experience that Spark is trying to collapse data into a single partition - suggest a way how to avoid that)
- 2A3, Try to explain why we are trying to avoid single partition and what are the consequences of having a single partition during the computation. (Think about how distributed computation in Spark works).

Use Spark with Python or Scala. For 2A1, 2A2 we want the actual code, 2A3 is a theoretical question.

### 2B, caching
Assume we have a parquet file with these three columns: col1, col2, col3 (all have numerical values). Next we create a Spark DataFrame as follows:
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
**Which of these three queries will take the data from cache? Please explain your answer.**

### 2C, theoretical

Partitioning and bucketing in Spark:
- 2C1, What is the purpose and how is partitioning and bucketing used in Spark? In this case we mean file system / output partitioning.
- 2C2, What are the differences between them and what are the situation one or another is used (or both)?
- 2C3, Are there any side effects one should be aware of in practical usage?
