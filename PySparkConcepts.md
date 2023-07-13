
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

* Why is spark Better than hadoop
	* Better in iteration as spark 
		* allows caching and 
		* creates a lineage of jobs basically aided by its in memory computation which helps in reducing repeated  read and writes to the disk
		* Also executors and JVM are not killed but reused
		* Better and clean code with less amount of lines required compared to mapReduce using scala in hadoop
	*  whereas in Hadoop as each Iteration happens read write to disk and JVM recreation happens which slows it down significantly 
	
* Scala vs PySpark
	* Scala is always better in case performance is key requirement.Scala UDF's are better considering reduced amount of object handling . Scala it TypeSafe .
	* Pyspark codes are easy to understand , learn and maintain with huge library support. Not TypeSafe .

* [ Driver program ( SparkContext )] 
		* [Cluster Manger]
			* [Worker (executor)]
			* [Worker (executor)]
			* [Worker (executor)]

* What is Spark Execution Model? 
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

* executor is a yarn container running on a node/slave and executor core is a number of threads runnning on your executor. 
	* executor is the jvm container and executor cores are threads

* SparkContext vs SparkSession
	* Earlier than spark2 we use to create multiple sparkContext but later we came uip with a concept of mutiple SparkSessions for multiple users but SparkContext will be limited to be 1 .
	* SparkSession now has SparkContext,SQLContext,HiveContext within.
	* Different SparkSession variable will refer to same spark context . 

* Why different SparkSession is needed ?
	* each user will be able to have their own properties set.
	* certain tables will be owned per users.
	* cluster will still be available as a shared resource.

* why spark dataframe ?
	* there are 3 options available -> RDD , Dataframe , Dataset
	* A dataframe is a dataset organized into named columns and is conceptually equivalent to tables in Relational databases
	* Dataframe do not mandate type
	* auto-optimised conversion to rdd's
	* catalyst optimizer helps in code optimisation
	* sparkSQL
	* wide language support
	* wide support for Datasource API thus reading multiple formats easy

* spark datasets
	* introduced in 1.6
	* TypeSafety - compile tyme typesafety
	* Direct operation over user defined classes
	* RDD functional nature and Dataframe's optimizations
	* Datasets are more memory optimized

* In yarn client mode driver runs on machine from which code was submitted and in cluster mode driver runs on one of the cluster node.

* RDD [ Resilient distributed datasets ]
	* Resilient [Fault tolerable as it can be recreated with same set of instructions as applied earlier ]
	* Distributed [ partirioned data and each of this part data is on a different machine  ]
	* Immutable [once a operation is performed on an RDD a new RDD is created ]
	* Dataset

* why is spark fault tolerance ?
	* spark lineage helps in making spark fault tolerant.
	* As rdd is immutable any lost partitioned can be tracked back and recreated as individual rdd's are not modified and can be persisted or cached as required .

* what is lineage in spark ?
	* spark will store information over sequence of operation to restore any possible **rdd X** and this information sequence is called lineage. This information is used for creating execution plan.
	* set of steps to regenerate a particular RDD .
	* logical plan 
	
* what is DAG in spark ?
	* Directed Acyclic graph
	* As soon as a action is taken lineage is submited to DAG scheduler and will convert same to a physical plan. 
	* physical plan will depend upon taks involved.

* cache vs persist in spark
	* while using cache spark will store data in RAM as a deserialized java object and portion of data will be lost is memory is less than the size of rdd.
	* persistance is methodology to store data for an rdd based on userinput and we can also write data to disk incase ample memory is not available . 


* Difference between spark and yarn fault tolerances 
	* yarn being resource manager , it restarts Application manager and containers as required.
	* spark being an execution framework helps in recovering data partition utilizing lineage which is available.

* Lazy Evaluation is an evaluation strategy that delays the evaluation of an expression until its value is needed. Eager Evaluation is the evaluation strategy you'll most probably be familiar with and is used in most programming languages.

* Why lazy evaluvation is important or required ?
	* Spark does not process data unless **action** is being called as when it is called spark will create most efficient path for achieving transformations upto the mentioned steps.

* Transformation vs Actions
	* `Transformations` are the functions which take RDD as input and produce one or more RDDs as output. Some e.g. are `map()`, `filter()` etc. One thing which we should note that whenever any `transformation` or series of `transformation` functions are called then the result is not produced immediately instead they are lazily evaluated and instead of producing new RDDs immediately, DAG(Direct Acyclic Graph) is created with input RDD and functions called. This graph will be kept on building until some  `Action` functions like `collect()`, `take()` is triggered.
	
* Transormations in detail
	* Narrow transformations
		* only one partition data is required to calculate the result
		* no shuffle happens
		* Map() , filter()
		* let say we have partitons by some field and we only apply changes to values which will belong to one particular partitions
	* wide transforamtion
		* across partition transformation and many/all partitons are required
		* This will also lead to a shuffle
		* aggregate functions and groupBy() are one of the major examples
	
* map() vs flatMap()
	* map() transformation applies a function to each row in a DataFrame/Dataset and returns the new transformed Dataset
	* flatMap() transformation flattens the DataFrame/Dataset after applying the function on every element and returns a new transformed Dataset which will have more rows than the current. 

* map() vs mapPartitions()
	* map() is applied to each row 
	* mapPartitions is applied to partition and advantage is we are making one dB connection per partition and not per row which helps in improving performance.

* groupBy() and reduceBy ()
	* reduceBy() does use combiner and are more efficient
	* groupBy() does not use combiner leads to shuffle

* AggregrateByKey() 
	* AggregrateByKey() is an operation used on pair rdd's [key value]
	* needs 3 param [initial value , combiner functions , merge functions ]
	* usually used for associative and commutative

* CombineByKey()
	* groupBy key is expensive as every single key pair will be shuffeled 
	* combineby Key is one optimizaions
	* more generic than aggregateByKey
	* need 3 params [create combiner , combiner functions , merge function ] 
	
* Accumulator in spark
	*  used to aggregate the information of particular collection
	*  normally used for stats about the job
	*  executor writed accumulator values and are not supposed read same whereas it is a responsibility of driver

* Broadcast variable in spark
	* read only variables
	* we can call destroy method for workers to stop using broadcast variable
		*{sc.bradcast(m)}
			{m}{worker1}
			{m}{worker2}
			{m}{worker3}
			
* Spark GLOM
	* it is an operation which helps us to treat all the values in an partition as an array
	* useful for linear algebra operation and in ML operation
	* it utilizes matrix structure operations instead of row by row 

* spark UDF
	* ad a non existing functionality
	* it is a very costly operation as udf are not optimised by spark 
		* `there are 22 udf classes in java and 1,2,3 represent how many inputs we can make`-review
* .queryExecution.executedPlan helps you see what logical plan was followed for resultant rdd
* udf are processed on executors

* spark vectorized udf 
	* earlier to 2.3 in case of udf use row by row executions were made and so pandas udf were utilized to bring spark vectorized functions which resulted into a huge performance improvement.
		* @pandas_udf annotation is utilized 
	* apache arrow is utilized for cross language in memory code transformation

* 


###spark config options

*	Spark.sql.parquet.filterPushdown
*	


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
* 	
	
