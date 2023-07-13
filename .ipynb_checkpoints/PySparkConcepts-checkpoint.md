Imports in PySpark
* pyspark.sql.SparkSession            – SparkSession is the main entry point for DataFrame and SQL functionality.
* pyspark.sql.DataFrame               – DataFrame is a distributed collection of data organized into named columns.
* pyspark.sql.Column                  – A column expression in a DataFrame.
* pyspark.sql.Row                     – A row of data in a DataFrame.
* pyspark.sql.GroupedData             – An object type that is returned by DataFrame.groupBy().
* pyspark.sql.DataFrameNaFunctions    – Methods for handling missing data (null values).
* pyspark.sql.DataFrameStatFunctions  – Methods for statistics functionality.
* pyspark.sql.functions               – List of standard built-in functions.
* pyspark.sql.types                   – Available SQL data types in PySpark.
* pyspark.sql.Window                  – Would be used to work with window functions.

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
	* 	


**sparkContext** is a Scala implementation entry point and JavaSparkContext is a java wrapper of sparkContext.

SQLContext is entry point of SparkSQL which can be received from sparkContext.Prior to 2.x.x, RDD ,DataFrame and Data-set were three different data abstractions.Since Spark 2.x.x, 
All three data abstractions are unified and SparkSession is the unified entry point of Spark.

