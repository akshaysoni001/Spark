#Default 10 Parition created by spark
dfretail=spark.read.format("csv").option("inferSchema","True").option("header","True").load('/home/akshay/Downloads/retail-data/by-day/*.csv')

#Partitioning Manually
dfretailrepartition=spark.read.format("csv").option("inferSchema","True").option("header","True").load('/home/akshay/Downloads/retail-data/by-day/*.csv').repartition(15)

# Decrease partition using COalesce
dfretailcoalesce=spark.read.format("csv").option("inferSchema","True").option("header","True").load('/home/akshay/Downloads/retail-data/by-day/*.csv').coalesce(5)


# WithOut InferSchema
dfretail1=spark.read.format("csv").option("header","True").load('/home/akshay/Downloads/retail-data/by-day/*.csv')


dfretail.select(ltrim(lit("    HELLLO   ")),
				rtrim(lit("    HELLO    ")),
				trim(lit("    HELLO    ")),
				lpad(lit("HELLO"),10,"$"),
				rpad(lit("HELLO"),10,"$"),
				pad(lit("HELLO"),10,"$")
				).show(2)


reg_string="BLACK|WHITE|RED|GREEN|BLUE|PINK"

dfretail.select(
	regexp_replace(col("Description"),reg_string,"500").alias("Color_Clean"),col("Description")
	).show(25,False)

dfretail.select(translate(col("Description"),"LEET","1337"),col("Description")).show(5,False)

extract_str="(BLACK|WHITE|RED|GREEN|BLUE|PINK)"

dfretail.select(
	regexp_extract(col("Description"),extract_str,1).alias("Color_Clean"),col("Description")
	).show(5,False)



## Date & TimeStamp

dateDF=spark.range(10).withColumn("today",current_date()).withColumn("now",current_timestamp())

dateDF.select(date_sub(col("today"),5),date_add(col("today"),5)).show(2,False)

dateDF.select(date_sub(col("today"),30),date_add(col("today"),30)).show(2,False)

dfretailwithdate=dfretail.withColumn('DateCasted',to_date("InvoiceDate"))
dfretailwithtime=dfretail.withColumn('DateCasted',to_timestamps("InvoiceDate"))

dateDF.withColumn("WeekAgo",date_sub(col("today"),7)).select(datediff(col("today").col("weekAgo"))).show()




# Complex Data Types
from pyspark.sql.functions import struct
complexDf=dfretail.select(struct("Description","InvoiceNo").alias("Complex"))

from pyspark.sql.functions import split

dfretail.select(split(col("Description")," ").alias("array_col")).show(5,False)

dfretail.select(split(col("Description")," ").alias("array_col")).selectExpr("array_col[0]").show(5,False)

from pyspark.sql.functions import size
dfretail.select(size(split(col("Description")," "))).show(5)

from pyspark.sql.functions import array_contains
dfretail.select(array_contains(split(col("Description"),' '),"WHITE")).show(10,False)

from pyspark.sql.functions import split,explode
dfretail.withColumn("Splitted",split(col("Description")," ")).withColumn("Exploded",explode(col("Splitted"))).select("Splitted","Exploded").show(2,False)



# Map

from pyspark.sql.functions import create_map
dfretail.select(create_map(col("Description"),col("InvoiceNo")).alias("Complex Map")).show(5,False)

dfretail.select(create_map(col("InvoiceNo"),col("Description")).alias("Complex Map")).show(5,False)


dfretail.select(create_map(col("Description"),col("InvoiceNo")).alias("Complex Map")).selectExpr("complex map['WHITE METAL LANTERN']").show(5,False)

## Python Funtions

UdfExampleDF=spark.range(5).toDF("num")

def power3(val):
	return val ** 3

#REgistering functions with spark
from pyspark.sql.functions import Udf
power3udf=udf(power3)

from pyspark.sql.functions import col
UdfExampleDF.select(power3udf(col("num"))).show(2)

UdfExampleDF.select(power3(col("num"))).show(2)

UdfExampleDF.selectExpr("power3udf(num)").show() --> Error function is not registered

from spark.sql.types import *
# Wrong --> udf("power3py","power3","Long Type")


spark.udf.register("power3py",power3,LongType())
UdfExampleDF.selectExpr("power3py(num)").show(2)


# File Formats 

df=spark.read.format("JSON").load('/home/akshay/Downloads/flight-data/json/2015-summary.json')
df1=spark.read.format("CSV").load('/home/akshay/Downloads/flight-data/csv/2015-summary.csv')

# Columns is not coming in CSV
df1=spark.read.format("CSV").option("header","true").option("inferSchema","true").load('/home/akshay/Downloads/flight-data/csv/2015-summary.csv')



# ReadModes
# Default is PERMISSIVE
dfreaddefault = spark.read.format("JSON").load('/home/akshay/Downloads/flight-data/readmode/2015-summary.json')


# PERMISSIVE
dfreadper = spark.read.format("JSON").option("mode","permissive").load('/home/akshay/Downloads/flight-data/readmode/2015-summary.json')


# DropMalformed
dfreadmal = spark.read.format("JSON").option("mode","dropmalformed").load('/home/akshay/Downloads/flight-data/readmode/2015-summary.json')

# FailFast
dfreadfail= spark.read.format("JSON").option("mode","failfast").load('/home/akshay/Downloads/flight-data/readmode/2015-summary.json')



schema=StructType(StructField("DEST_COUNTRY_NAME",StringType(),"True"),StructField("ORIGIN_COUNTRY_NAME",StringType(),"True"),StructField("count",LongType(),"True"))
dfreadpersch = spark.read.format("JSON").option("mode","permissive").schema(schema).load('/home/akshay/Downloads/flight-data/readmode/2015-summary.json')



dfoscar=spark.read.format("CSV").option("inferSchema","True").load('/home/akshay/Downloads/oscarfile.csv')


os=StructType([StructField("Index",LongType(),True),StructField("Year",LongType(),True),StructField("Age",LongType(),True),StructField('Name',StringType(),True),StructField('Movie',StringType(),True),StructField("_corrupt_record",StringType(),True)])
dfoscar3=spark.read.format("CSV").option("header","true").option("ignoreLeadingWhiteSpace","true").load('/home/akshay/Downloads/oscarfile.csv')


dfoscar3.write.format("csv").mode("overwrite").option("sep","\t").save("/home/akshay/Downloads/flight-data/processed.tsv")




# Parque File Format

dfpq=spark.read.format("parquet").load('/home/akshay/Downloads/flight-data/parquet/2010-summary.parquet')
dfpq1=spark.read.parquet().load('/home/akshay/Downloads/flight-data/parquet/2010-summary.parquet')

## Structured Database


Create table cats (
	id INT unsigned not null auto_increment primary key,
	name varchar(150) not null,
	owner varchar(150) not null,
	birth Date not null);

insert into cats(name,owner,birth) values('sandy','Lennon','2015-01-03'),
('Cookie','Casey','2013-11-13'),
('Charlie','River','2016-05-21');


pyspark --driver-class-path mysql-connector-java-5.1.45-bin.jar --jars mysql-connector-java-5.1.45-bin.jar

MySqlDFcats=spark.read.format('jdbc').option("driver","com.mysql.jdbc.Driver").option('url','jdbc:mysql://127.0.0.1:3306/pets?useSSL=false').option('dbtable','cats').option('user','akshay').option('password','akshay').load()


ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root';


squery='(select DISTINCT(owner) from cats) as cats'
MySqlDFqr=spark.read.format('jdbc').option("driver","com.mysql.jdbc.Driver").option('url','jdbc:mysql://127.0.0.1:3306/pets?useSSL=false').option('dbtable',squery).option('user','root').option('password','root').load()


MySqlDFqr=spark.read.format('jdbc').option("driver","com.mysql.jdbc.Driver").option('url','jdbc:mysql://127.0.0.1:3306/pets?useSSL=false').option('dbtable','(select DISTINCT(owner) from cats) as cats').option('user','root').option('password','root').load()


q='(Create table cats (id INT unsigned not null auto_increment primary key,name varchar(150) not null,owner varchar(150) not null,birth Date not null)) as tab';

new=spark.write.format('jdbc').option("driver","com.mysql.jdbc.Driver").option('url','jdbc:mysql://127.0.0.1:3306/pets?useSSL=false').option('dbtable',q).option('user','root').option('password','root').load()


create table Dogs12 ( id int unsigned not null auto_increment primary key,name varchar(150) not null,owner varchar(150) not null, birth date not null)


insert into Dogs12 (name,owner,birth) values('Tommy','Lennon','2015-03-01'),('Gunda','Brahmachari','2013-11-13'),('Raja','Radha','2016-05-21');



MySqlDFDogs12=spark.read.format('jdbc').option('driver','com.mysql.jdbc.Driver').option('url','jdbc:mysql://127.0.0.1:3306/pets?useSSL=false').option('dbtable','Dogs1').option('user','root').option('password','root').load()


newRows=[Row(4,'Doggy1','Akshay','2021-08-11'),Row(5,'Doggy2','Arpit','2021-10-12')]


prows=spark.sparkContext.parallelize(newRows1)
newdog1=spark.createDataFrame(prows,schema)
newdog1.write.format('jdbc').option('driver','com.mysql.jdbc.Driver').option('url','jdbc:mysql://127.0.0.1:3306/pets?useSSL=false').option('dbtable','Dogs1').option('user','root').option('password','root').mode('append').save()


newdog1.write.format('jdbc').option('driver','com.mysql.jdbc.Driver').option('url','jdbc:mysql://127.0.0.1:3306/pets?useSSL=false').option('dbtable','Dogs12').option('user','root').option('password','root').mode('append').save()
newRows1=[Row(4,'Doggy1','Akshay',2021-08-11),Row(5,'Doggy2','Arpit',2021-10-12)]

insert into Dogs12 (name,owner,birth) values('Tommy','Lennon',2015-03-01),('Gunda','Brahmachari',2013-11-13),('Raja','Radha',2016-05-21);

MySqlDFDogs12=spark.read.format('jdbc').option('driver','com.mysql.jdbc.Driver').option('url','jdbc:mysql://127.0.0.1:3306/pets?useSSL=false').option('dbtable','Dogs12').option('user','root').option('password','root').load()

newRows1=[Row(4,'Doggy1','Akshay','2021-08-11'),Row(5,'Doggy2','Arpit','2021-10-12')]

newdog1=spark.createDataFrame(prows,schema)
newRows1=[Row(4,'Doggy1','Akshay','2021-08-11'),Row(5,'Doggy2','Arpit','2021-10-12')]

newdog1.write.format('jdbc').option('driver','com.mysql.jdbc.Driver').option('url','jdbc:mysql://127.0.0.1:3306/pets?useSSL=false').option('dbtable','Dogs12').option('user','root').option('password','root').mode('append').save()



newRows1=[Row(4,'Doggy1','Akshay',datetime.strptime('2019-12-01','%Y-%m-%d')),Row(5,'Doggy2','Arpit',datetime.strptime('2021-12-11','%Y-%m-%d'))]
prows1=spark.sparkContext.parallelize(newRows1)
newdog1=spark.createDataFrame(prows1,schema1)
newdog1=spark.createDataFrame(prows,schema)

MySqlDFcats=spark.read.format('jdbc').option('driver','com.mysql.jdbc.Driver').option('url','jdbc:mysql://127.0.0.1:3306/pets?useSSL=false').option('dbtable','cats').option('user','root').option('password','root').option('numPartitions',10).load()




muldf=spark.read.format("csv").option('header','true').option('inferSchema','true').load('/home/akshay/Downloads/retail-data/by-daye/*')
nulldf=spark.read.format("csv").option('header','true').option('inferSchema','true').load('/home/akshay/Downloads/checknullfile.csv')





from pypark.sql.functions import *
	muldf.select(
		sum("Quantity").alias("SumOfQuantity"),
		avg("Quantity").alias("AvgofQuantity"),
		count("Quantity").alias("CountofQuantity"),
		expr("mean(Quantity)").alias("meanofQuantity")

		).selectExpr("SumOfQuantity/CountofQuantity","AvgofQuantity","meanofQuantity").show()
# 	


muldf.select(var_pop("Quantity"),var_samp("Quantity"),stddev("Quantity"),stddev_pop("Quantity"),stddev_samp("Quantity")).show()

muldf.select(skewness("Quantity"),kurtosis("Quantity")).show()

muldf.select(corr("Quantity","InvoiceNo"),covar_pop("Quantity","InvoiceNo"),covar_samp("Quantity","InvoiceNo")).show()


muldf.groupby("InvoiceNo").agg(count("Quantity").alias("quan"),expr("count(Quantity)")).show(10)


## Window Functions
from pyspark.sql.window import Window
from pyspark.sql.functions import *

windowSpec=Window.partitionBy("CustomerId","date").orderBy(desc("Quantity")).rowsBetween(Window.unboundedPreceding,Window.currentRow)

maxPurchaseQuantity=max(col("Quantity")).over(windowSpec)

purchasedDenseRank = dense_rank().over(windowSpec)
PurchaseRank = rank().over(windowSpec)

dfWithDate = muldf.withColumn("date",to_date(col("InvoiceDate")))
dfretailwithdate.where("CustomerId is not null").orderBy("CustomerId").select(col("CustomerId"),col("date"),col("Quantity"),purchasedDenseRank.alias("QuantityDenseRank"),PurchaseRank.alias("QuantityRank"),maxPurchaseQuantity.alias("maxPurchaseQuantity")).show(60)



# RollUp & Cube
dfNoNull=dfWithDate.drop()
rolledUpDf=dfNoNull.rollup("date","Country").agg(sum("Quantity")).selectExpr("date","Country","`sum(Quantity)`").orderBy("date")
rolledUpDf.where("Country is NULL")
rolledUpDf.wher("date is NULL")

rolledUpDf1=dfNoNull.rollup("Country","date").agg(sum("Quantity")).selectExpr("date","Country","`sum(Quantity)`").orderBy("date")



cubeDf=dfNoNull.cube("Country","date").agg(sum("Quantity")).selectExpr("date","Country","`sum(Quantity)`").orderBy("date")





## Join

person.join(graduateProgram,joinExpr,"cross").show()


#Duplicates
graduateProgdup = graduateProgram.withColumnRenamed("id","graduate_program")
joinExpr=person["graduate_program"]==graduateProgdup["graduate_program"]
join_type="inner"
graduateProgdup.join(person,joinExpr,join_type).show()

person.join(graduateProgdup,joinExpr,join_type).select("graduate_program","degree","department","School")



# Drop the duplicate Column

person.join(graduateProgdup,joinExpr,join_type).drop(person["graduate_program"]).show()


# Rename the Duplicate column name
graduateProgdupColRenamed=graduateProgdup.withColumnRenamed("graduate_program","GradId")

joinExpr=person["graduate_program"]==graduateProgdupColRenamed["GradId"]


person.join(graduateProgdupColRenamed,joinExpr,join_type)



