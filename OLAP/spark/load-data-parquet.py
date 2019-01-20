from pyspark.sql.types import StructType, StructField, IntegerType,StringType ,DecimalType,FloatType,DateType;

fields=StructType([StructField("N_NATIONKEY", IntegerType(), True), StructField("N_NAME", StringType(), True), StructField("N_REGIONKEY", IntegerType(), True), StructField("N_COMMENT", StringType(), True)])
table = sc.textFile("/data/OLAP_Benchmark_data/nation.tbl")
df = table.map(lambda x: x.split("|")).map(lambda x: {'N_NATIONKEY':int(x[0]),'N_NAME':x[1],'N_REGIONKEY':int(x[2]),'N_COMMENT':x[3]}).toDF(fields)
df.write.mode("overwrite").parquet("hdfs://namenode:8020/nation.parquet")

fields=StructType([StructField("R_REGIONKEY", IntegerType(), True), StructField("R_NAME", StringType(), True), StructField("R_COMMENT", StringType(), True)])
table = sc.textFile("/data/OLAP_Benchmark_data/region.tbl")
df = table.map(lambda x: x.split("|")).map(lambda x: {'R_REGIONKEY':int(x[0]),'N_NAME':x[1],'N_COMMENT':x[2]}).toDF(fields)
df.write.mode("overwrite").parquet("hdfs://namenode:8020/region.parquet")

fields=StructType([StructField("S_SUPPKEY", IntegerType(), True), StructField("S_NAME", StringType(), True), StructField("S_ADDRESS", StringType(), True),StructField("S_NATIONKEY", IntegerType(), True), StructField("S_PHONE", StringType(), True), StructField("S_ACCTBAL", FloatType(), True), StructField("S_COMMENT", StringType(), True)])
table = sc.textFile("/data/OLAP_Benchmark_data/supplier.tbl")
df = table.map(lambda x: x.split("|")).map(lambda x: {'S_SUPPKEY':int(x[0]),'S_NAME':x[1],'S_ADDRESS':x[2],'S_NATIONKEY':int(x[3]),'S_PHONE':x[4],'S_ACCTBAL':float(x[5]),'S_COMMENT':x[6]}).toDF(fields)
df.write.mode("overwrite").parquet("hdfs://namenode:8020/supplier.parquet")

fields=StructType([StructField("PS_PARTKEY", IntegerType(), True), StructField("PS_SUPPKEY", IntegerType(), True), StructField("PS_AVAILQTY", IntegerType(), True),StructField("PS_SUPPLYCOST", FloatType(), True), StructField("PS_COMMENT", StringType(), True)])
table = sc.textFile("/data/OLAP_Benchmark_data/partsupp.tbl")
df = table.map(lambda x: x.split("|")).map(lambda x: {'PS_PARTKEY':int(x[0]),'PS_SUPPKEY':int(x[1]),'PS_AVAILQTY':int(x[2]),'PS_SUPPLYCOST':float(x[3]),'PS_COMMENT':x[4]}).toDF(fields)
df.write.mode("overwrite").parquet("hdfs://namenode:8020/partsupp.parquet")

fields=StructType([StructField("P_PARTKEY", IntegerType(), True), StructField("P_NAME", StringType(), True), StructField("P_MFGR", StringType(), True),StructField("P_BRAND", StringType(), True), StructField("P_TYPE", StringType(), True), StructField("P_SIZE", IntegerType(), True), StructField("P_CONTAINER", StringType(), True), StructField("P_RETAILPRICE", FloatType(), True), StructField("P_COMMENT", StringType(), True)])
table = sc.textFile("/data/OLAP_Benchmark_data/part.tbl")
df = table.map(lambda x: x.split("|")).map(lambda x: {'P_PARTKEY':int(x[0]),'P_NAME': x[1],'P_MFGR':x[2],'P_BRAND':x[3],'P_TYPE':x[4],'P_SIZE':int(x[5]),'P_CONTAINER':x[6],'P_RETAILPRICE':float(x[7]),'P_COMMENT':x[8]}).toDF(fields)
df.write.mode("overwrite").parquet("hdfs://namenode:8020/part.parquet")

fields=StructType([StructField("C_CUSTKEY", IntegerType(), True), StructField("C_NAME", StringType(), True), StructField("C_ADDRESS", StringType(), True),StructField("C_NATIONKEY", IntegerType(), True), StructField("C_PHONE", StringType(), True), StructField("C_ACCTBAL", FloatType(), True), StructField("C_MKTSEGMENT", StringType(), True), StructField("C_COMMENT", StringType(), True)])
table = sc.textFile("/data/OLAP_Benchmark_data/customer.tbl")
df = table.map(lambda x: x.split("|")).map(lambda x: {'C_CUSTKEY':int(x[0]),'C_NAME': x[1],'C_ADDRESS':x[2],'C_NATIONKEY':int(x[3]),'C_PHONE':x[4] ,'C_ACCTBAL':float(x[5]),'C_MKTSEGMENT':x[6],'C_COMMENT':x[7]}).toDF(fields)
df.write.mode("overwrite").parquet("hdfs://namenode:8020/customer.parquet")

fields=StructType([StructField("O_ORDERKEY", IntegerType(), True), StructField("O_CUSTKEY", IntegerType(), True), StructField("O_ORDERSTATUS", StringType(), True),StructField("O_TOTALPRICE", FloatType(), True), StructField("O_ORDERDATE", StringType(), True), StructField("O_ORDERPRIORITY", StringType(), True), StructField("O_CLERK", StringType(), True), StructField("O_SHIPPRIORITY", IntegerType(), True), StructField("O_COMMENT", StringType(), True)])
table = sc.textFile("/data/OLAP_Benchmark_data/orders.tbl")
df = table.map(lambda x: x.split("|")).map(lambda x: {'O_ORDERKEY':int(x[0]),'O_CUSTKEY': int(x[1]),'O_ORDERSTATUS':x[2],'O_TOTALPRICE':float(x[3]),'O_ORDERDATE':x[4] ,'O_ORDERPRIORITY':x[5],'O_CLERK':x[6],'O_SHIPPRIORITY':int(x[7]),'O_COMMENT':x[8]}).toDF(fields)
df.write.mode("overwrite").parquet("hdfs://namenode:8020/orders.parquet")

fields=StructType([StructField("L_ORDERKEY", IntegerType(), True), StructField("L_PARTKEY", IntegerType(), True), StructField("L_SUPPKEY", IntegerType(), True),StructField("L_LINENUMBER", IntegerType(), True), StructField("L_QUANTITY", FloatType(), True), StructField("L_EXTENDEDPRICE", FloatType(), True), StructField("L_DISCOUNT", FloatType(), True), StructField("L_TAX", FloatType(), True), StructField("L_RETURNFLAG", StringType(), True), StructField("L_LINESTATUS", StringType(), True), StructField("L_SHIPDATE", StringType(), True), StructField("L_COMMITDATE", StringType(), True), StructField("L_RECEIPTDATE", StringType(), True), StructField("L_SHIPINSTRUCT", StringType(), True), StructField("L_SHIPMODE", StringType(), True), StructField("L_COMMENT", StringType(), True)])
table = sc.textFile("/data/OLAP_Benchmark_data/lineitem.tbl")
df = table.map(lambda x: x.split("|")).map(lambda x: {'L_ORDERKEY':int(x[0]),'L_PARTKEY': int(x[1]),'L_SUPPKEY':int(x[2]),'L_LINENUMBER':int(x[3]),'L_QUANTITY':float(x[4]) ,'L_EXTENDEDPRICE':float(x[5]),'L_DISCOUNT':float(x[6]),'L_TAX':float(x[7]),'L_RETURNFLAG':x[8],'L_LINESTATUS':x[9],'L_SHIPDATE':x[10],'L_COMMITDATE':x[11],'L_RECEIPTDATE':x[12],'L_SHIPINSTRUCT':x[13],'L_SHIPMODE':x[14],'L_COMMENT':x[15]}).toDF(fields)
df.write.mode("overwrite").parquet("hdfs://namenode:8020/lineitem.parquet")

import time
from pyspark.sql import functions as func
nation = spark.read.load("hdfs://namenode:8020/nation.parquet", format="parquet");
region = spark.read.load("hdfs://namenode:8020/region.parquet", format="parquet");
supplier = spark.read.load("hdfs://namenode:8020/supplier.parquet", format="parquet");
partsupp = spark.read.load("hdfs://namenode:8020/partsupp.parquet", format="parquet");
part = spark.read.load("hdfs://namenode:8020/part.parquet", format="parquet");
customer = spark.read.load("hdfs://namenode:8020/customer.parquet", format="parquet");
orders = spark.read.load("hdfs://namenode:8020/orders.parquet", format="parquet");
lineitem = spark.read.load("hdfs://namenode:8020/lineitem.parquet", format="parquet");