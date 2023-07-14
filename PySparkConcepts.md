
### Imports in PySpark

* **pyspark.sql.SparkSession**            – SparkSession is the main entry point for DataFrame and SQL functionality.
* **pyspark.sql.DataFrame**               – DataFrame is a distributed collection of data organized into named columns.
* **pyspark.sql.Column**                  – A column expression in a DataFrame.
* **pyspark.sql.Row**                     – A row of data in a DataFrame.
* **pyspark.sql.GroupedData**             – An object type that is returned by DataFrame.groupBy().
* **pyspark.sql.DataFrameNaFunctions**    – Methods for handling missing data (null values).
* **pyspark.sql.DataFrameStatFunctions**  – Methods for statistics functionality.
* **pyspark.sql.functions**               – List of standard built-in functions.
* **pyspark.sql.types**                   – Available SQL data types in PySpark.
* **pyspark.sql.Window**                  – Would be used to work with window functions.

from pyspark.sql.types import *
* 	StringType	ShortType
* 	ArrayType	IntegerType
* 	MapType	LongType
* 	StructType	FloatType
* 	DateType	DoubleType
* 	TimestampType	DecimalType
* 	BooleanType	ByteType
* 	CalendarIntervalType	HiveStringType
* 	BinaryType	ObjectType
* 	NumericType	NullType

from pyspark.sql.functions import *


### Spark Interview Notes

* **Why is spark Better than hadoop**
	* Better in iteration as spark 
		* allows caching and 
		* creates a lineage of jobs basically aided by its in memory computation which helps in reducing repeated  read and writes to the disk
		* Also executors and JVM are not killed but reused
		* Better and clean code with less amount of lines required compared to mapReduce using scala in hadoop
	*  whereas in Hadoop as each Iteration happens read write to disk and JVM recreation happens which slows it down significantly 

* **Scala vs PySpark**
	* Scala is always better in case performance is key requirement.Scala UDF's are better considering reduced amount of object handling . Scala it TypeSafe .
	* Pyspark codes are easy to understand , learn and maintain with huge library support. Not TypeSafe .

* [ Driver program ( SparkContext )] 
	* [Cluster Manger]
	* [Worker (executor)]
	* [Worker (executor)]
	* [Worker (executor)]

* Directed acyclic graph
	* The DAG is “directed” because the operations are executed in a specific order, and 
	* “acyclic” because there are no loops or cycles in the execution plan

* **What is Spark Execution Model?** 
	* Driver machine is where spark's main program runs. It initiates spark context, stores spark conf. 
	* Spark context is a bridge between cluster manager and driver. Cluster manager is a resource manager [Three types used: Standalone,  Mesos, Yarn.] 
	* Spark context negotiates resources with cluster manager and submits jobs to cluster. 
		* Driver [controlling body]
			* creates a logical plan, a DAG and an execution plan. 
			* Logic is divided into stages which is further divided into tasks. 
			* Driver decides which executor runs which task.
		* Workers [execution body]
			* are physical machines, and multiple executors can run on the same worker. 
			* Every executor can have multiple cores to run tasks in parallel.

* **executor vs executor core**
* executor is a yarn container running on a node/slave and executor core is a number of threads runnning on your executor. 
	* executor is the jvm container and executor cores are threads

* **SparkContext vs SparkSession**
	* Earlier than spark2 we use to create multiple sparkContext but later we came uip with a concept of mutiple SparkSessions for multiple users but SparkContext will be limited to be 1 .
	* SparkSession now has SparkContext,SQLContext,HiveContext within.
	* Different SparkSession variable will refer to same spark context . 

* **Why different SparkSession is needed ?**
	* each user will be able to have their own properties set.
	* certain tables will be owned per users.
	* cluster will still be available as a shared resource.

* **why spark dataframe ?**
	* there are 3 options available -> RDD , Dataframe , Dataset
	* A dataframe is a dataset organized into named columns and is conceptually equivalent to tables in Relational databases
	* Dataframe do not mandate type
	* auto-optimised conversion to rdd's
	* catalyst optimizer helps in code optimisation
	* sparkSQL
	* wide language support
	* wide support for Datasource API thus reading multiple formats easy

* **spark datasets**
	* introduced in 1.6
	* TypeSafety - compile tyme typesafety
	* Direct operation over user defined classes
	* RDD functional nature and Dataframe's optimizations
	* Datasets are more memory optimized
	* 

* In yarn client mode driver runs on machine from which code was submitted and in cluster mode driver runs on one of the cluster node.

* **RDD [ Resilient distributed datasets ]**
	* Resilient [Fault tolerable as it can be recreated with same set of instructions as applied earlier ]
	* Distributed [ partirioned data and each of this part data is on a different machine  ]
	* Immutable [once a operation is performed on an RDD a new RDD is created ]
	* Dataset

| RDD                                    | DataFrame                          | Datasets              |
| -------------------------------------- | ---------------------------------- | --------------------- |
| TypeSafe                               | Not TypeSafe                       | TypeSafe              |
| developer need to optimize             | catalyst optimizer                 | auto optimized        |
| not as good as datasets in performance | Performace not as good as datasets | better performance    |
| not memory efficient                   | not memory efficient               | More memory efficient |



* **why is spark fault tolerance ?**
	* spark lineage helps in making spark fault tolerant.
	* As rdd is immutable any lost partitioned can be tracked back and recreated as individual rdd's are not modified and can be persisted or cached as required .

* **what is lineage in spark ?**
	* spark will store information over sequence of operation to restore any possible **rdd X** and this information sequence is called lineage. This information is used for creating execution plan.
	* set of steps to regenerate a particular RDD .
	* logical plan 
	
* **what is DAG in spark ?**
	* Directed Acyclic graph
	* As soon as a action is taken lineage is submited to DAG scheduler and will convert same to a physical plan. 
	* physical plan will depend upon taks involved.

* **cache vs persist in spark**
	* while using cache spark will store data in RAM as a deserialized java object and portion of data will be lost is memory is less than the size of rdd.
	* persistance is methodology to store data for an rdd based on userinput and we can also write data to disk incase ample memory is not available . 


* **Difference between spark and yarn fault tolerances** 
	* yarn being resource manager , it restarts Application manager and containers as required.
	* spark being an execution framework helps in recovering data partition utilizing lineage which is available.

* Lazy Evaluation is an evaluation strategy that delays the evaluation of an expression until its value is needed. Eager Evaluation is the evaluation strategy you'll most probably be familiar with and is used in most programming languages.

* **Why lazy evaluvation is important or required ?**
	* Spark does not process data unless **action** is being called as when it is called spark will create most efficient path for achieving transformations upto the mentioned steps.

* **Transformation vs Actions**
	* `Transformations` are the functions which take RDD as input and produce one or more RDDs as output. Some e.g. are `map()`, `filter()` etc. One thing which we should note that whenever any `transformation` or series of `transformation` functions are called then the result is not produced immediately instead they are lazily evaluated and instead of producing new RDDs immediately, DAG(Direct Acyclic Graph) is created with input RDD and functions called. This graph will be kept on building until some  `Action` functions like `collect()`, `take()` is triggered.

* **Transormations in detail**
	* Narrow transformations
		* only one partition data is required to calculate the result
		* no shuffle happens
		* Map() , filter()
		* let say we have partitons by some field and we only apply changes to values which will belong to one particular partitions
	* wide transforamtion
		* across partition transformation and many/all partitons are required
		* This will also lead to a shuffle
		* aggregate functions and groupBy() are one of the major examples
	
* **map() vs flatMap()**
	* map() transformation applies a function to each row in a DataFrame/Dataset and returns the new transformed Dataset
	* flatMap() transformation flattens the DataFrame/Dataset after applying the function on every element and returns a new transformed Dataset which will have more rows than the current. 

* **map() vs mapPartitions()**
	* map() is applied to each row 
	* mapPartitions is applied to partition and advantage is we are making one dB connection per partition and not per row which helps in improving performance.

* **groupBy() and reduceBy ()**
	* reduceBy() does use combiner and are more efficient
	* groupBy() does not use combiner leads to shuffle

* **AggregrateByKey()** 
	* AggregrateByKey() is an operation used on pair rdd's [key value]
	* needs 3 param [initial value , combiner functions , merge functions ]
	* usually used for associative and commutative

* **CombineByKey()**
	* groupBy key is expensive as every single key pair will be shuffeled 
	* combineby Key is one optimizaions
	* more generic than aggregateByKey
	* need 3 params [create combiner , combiner functions , merge function ] 
	
* **Accumulator in spark**
	*  used to aggregate the information of particular collection
	*  normally used for stats about the job
	*  executor writed accumulator values and are not supposed read same whereas it is a responsibility of driver

* **Broadcast variable in spark**
	* read only variables
	* we can call destroy method for workers to stop using broadcast variable
		*{sc.bradcast(m)}
			{m}{worker1}
			{m}{worker2}
			{m}{worker3}
			
* **Spark GLOM**
	* it is an operation which helps us to treat all the values in an partition as an array
	* useful for linear algebra operation and in ML operation
	* it utilizes matrix structure operations instead of row by row 

* **spark UDF**
	* ad a non existing functionality
	* it is a very costly operation as udf are not optimised by spark 
		* `there are 22 udf classes in java and 1,2,3 represent how many inputs we can make`-review
* .queryExecution.executedPlan helps you see what logical plan was followed for resultant rdd
* udf are processed on executors

* **spark vectorized udf** 
	* earlier to 2.3 in case of udf use row by row executions were made and so pandas udf were utilized to bring spark vectorized functions which resulted into a huge performance improvement.
		* @pandas_udf annotation is utilized 
	* apache arrow is utilized for cross language in memory code transformation

*  **cache and persist** 
	* Using cache() and persist() methods, Spark provides an optimization mechanism to store the intermediate computation of an RDD, DataFrame, and Dataset so they can be reused in subsequent actions(reusing the RDD, Dataframe, and Dataset computation result’s) 
	* 

*	**Repartition vs coalesce**
	*	repartitioning shuffles and is more resource consuming operation and will also try to balance out or equate the file size of each partitions
	*	whereas coaleasce is a method or reducing number of partitions in order to optimize and minimize size of partitions. 
	*	coalesce may not always shuffle the complete data
	*	coalesce() is used only to reduce the number of partitions. This is optimized or improved version of repartition() where the movement of the data across the partitions is lower using coalesce.

* **partitionBy and Bucketing**
	* Partitioning is a way to split data into separate folders on disk based on one or multiple columns. This enables efficient parallelism and partition pruning in Spark.
	* Bucketing is a way to assign rows of a dataset to specific buckets and collocate them on disk. This enables efficient wide transformations in Spark, as the data is already collocated in the executors correctly.

* **Catalyst optimizer** 
	* This is an logical optimisation service built in dataframe API's which after creating logical execution plan optimizes same based on processing similar to decision tree based on resource cost model



* **spark shuffle service**
	* transfer of data between executors is called shuffling
	* while this shuffling happens it consumes computing resources from executors and to prevent we have external shuffle service in place


* **Window functions** 
	* Window functions are used to calculate results such as the rank, row number e.t.c over a range of input rows.


* **rank vs dense_rank**
	* rank will increase based on number of rows which have passed but rank will only show change when new value is observed
	* dense rank will increase only when a new value is observed


* **Dynamic resource Allocation**
	* 

*	Spark.sql.parquet.filterPushdown

*   import org.apache.spark.util.SizeEstimator
	logInfo(SizeEstimator.estimate(yourlargeorsmalldataframehere))

* **Avro vs Parquet**
	* avro is good for schema scaling whereas parquet is column major and optimized for faster query



###Spark implementation of Join

* Shuffle Hash join
    1. data is partitioned
    2. shuffling of data is done to ensure partitions with same key are on same machine
    3. hashing and in memory join is done 
    4. result is aggregated

* Spark sort merge join
	* default join since spark 2.3
    1. sort the data based on join key
    2. merge by row by row comparison
    3. aggregate the result as we merge 



### Spark memory management
*	Spark has below sub divisions in executor memory [assume 1 GB is executor memory]
	*	reserved memory : 300 MB
	*	User memory : 0.25 of 1GB
	*	Spark Memory fraction : Execution memory and storage memory [share space]
		*	broadcast and cached rdd's is stored in storage memory
		*	computation data is stored in execution memory




### Possible optimisations we can make in spark

* prefer reduceBy key over groupBy key [avoid unnecessary calculations which use shuffle]
* Use tree reduce vs reduce -  recombination keeps happening between executors as per depth of the tree reduce defination
* use broadcast wherever applicable - default, the size of a broadcast variable can be 10 MB
* use spark 2.X
	* uses optimized code generation (catalyst optimizer)
	* better spark encoders
* use the right file format to improve spark performance
	* analytical data  -  Parquet
	* orc and rc are some other
	* avro is good for transactional or operations for which you need entire row data
	* avoid using csv json etc.
	* Right file compression
* Handle skewness [balanced partitions will help and allot similar resources to executors ]
	*  salting  -  create buckets for the key which is creating skewness to data by add randomization
*  enable spark shuffle service
*  use right resource allocation
*  enable dynamic resource allocation
*  prefer mapPartitions over map



### Possible causes of out of memory



### Notes

*sparkContext* is a Scala implementation entry point and JavaSparkContext is a java wrapper of sparkContext.

*SQLContext* is entry point of SparkSQL which can be received from sparkContext.
> Prior to 2.x.x, RDD ,DataFrame and Data-set were three different data abstractions.Since Spark 2.x.x, 
> All three data abstractions are unified and SparkSession is the unified entry point of Spark.

Important functions and keywords
* 	col 
* 	rand
* 	range
* 	when(condition,fillwith).otherwise(fillWithThis)


####Ad Hoc questions 

2. What is Skewing and how to fix it
3. Spark Submit and its configurations
4. why are rdd/partitions immutable in pyspark?
5. how to recover partiton what is profilers in pyspark?
6. common cause of lost and deleted partitions?
7. different type of serialization and why serialization is used?

TOdo 
cast
timestamp handling 

1  - sql window function 
2  - employee manager 
3  - second highest salary of employees ?? rank 
4  - second highest salary of employees of different organisation ?? rank patitioning/bucketting
5  - transpose ? 
6  - why not use dictionary 
