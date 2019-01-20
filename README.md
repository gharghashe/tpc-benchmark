# TPC-benchmark
<div dir="rtl">
هدف از انجام این پروژه مقایسه کارایی دیتابیس های مختلف در دو حوزه OLAP و OLTP می باشد، برای انجام این کار از دیتابیس های postgreSQL ، spark (با فرمت های parquet ، orc و  avro برای ذخیره سازی) و flink (تنها با فرمت avro برای ذخیره سازی) در حوزه OLAP و از دیتابیس های postgreSQL و cockroachDB در حوزه OLTP استفاده شده است.
</div>

## OLAP 
<div dir="rtl">
در این قسمت می خواهیم کارایی 3 دیتابیس postgreSQL ، spark و flink را با یکدیگر مقایسه کنیم، برای postgreSQL داده ها بر روی خود دیتابیس و  برای spark و flink داده ها بر رو hdfs قرار می دهیم.
</div>
<div dir="rtl">
همچنین داده ها برای spark به سه شکل متفاوت شامل  parquet ، orc و avro بر روی hdfs قرار می گیرد و از داده ذخیره شده در قالب avro برای spark در flink نیز استفاده می کنیم.
</div>
<div dir="rtl">
در هر قسمت ۲۲ کوئری داده شده به ازای هر دیتابیس و هر فرمت ذخیره شده اجرا می کنیم و نتایج آن را با یکدیگر مقایسه می کنیم.
</div>

### PostgreSQL 
<div dir="rtl">
در ابتدا وارد محیط container مربوط به postgre می شویم و با استفاده از دستور psql وارد محیط کاری postgre می شویم.
</div> 
<div dir="rtl">
با استفاده از دستور \o /result/r1.txt خروجی مورد نظر را می توان در فایل ذخیره کرد‌ ( نتیجه هر کوئری  را در یک فایل ذخیره می کنیم)
</div> 
<div dir="rtl">
سپس با دستور create table جداول را ایجاد می کنیم و با دستور زیر داده ها را در جداول load می کنیم:
</div>

```
 COPY store.PART (P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE,P_COMMENT,test) FROM '/data/OLAP_Benchmark_data/part.tbl' DELIMITER '|';
 ```
   
<div dir="rtl">
به طور مثال در دستور بالا load کردن جدول PART انجام گرفته است.
</div>
   
<div dir="rtl">
در نهایت هر یک از کوئری ها را با دستور EXPLANE با پارامتر های زیر اجرا می کنیم:
</div>
   

```
 EXPLAIN (ANALYZE true,VERBOSE true,COSTS true,BUFFERS true,TIMING false,FORMAT JSON)
 ```
 
 
<div dir="rtl">
   در نهایت می توان کوئری های تبدیل شده در OLAP/postgreSQL/query را اجرا کرد   
 </div>

### Spark

<div dir="rtl">
  در این قسمت اول با استفاده از دستور زیر وارد محیط کاری spark می شویم:
 </div>

```
 pyspark --num-executors 1  --executor-memory 10g
 ```
<div dir="rtl">
  در ابتدا type های مورد نظر را import می کنیم:
 </div>

```
 from pyspark.sql.types import StructType, StructField, IntegerType,StringType ,DecimalType ,FloatType, DateType
 ```

<div dir="rtl">
  سپس با استفاده از دستور زیر ساختار یک جدول را تعریف می کنیم، به طور مثال برای جدول region داریم:
 </div>

```
 fields= StructType ([StructField("R_REGIONKEY", IntegerType(), True), StructField("R_NAME", StringType(), True), StructField( "R_COMMENT",  StringType(), True)])
 ```

<div dir="rtl">
  حال فایل جدول را می خوانیم : 
 </div>

```
 table = sc.textFile("/data/OLAP_Benchmark_data/nation.tbl")
 ```
<div dir="rtl">
  حال فایل خوانده شده را آماده نوشتن در hdfs می کنیم(در قالب یک data frame در می آوریم): 
 </div>

```
 df = table.map(lambda x: x.split("|")).map(lambda x: {'N_NATIONKEY' :int(x[0]), 'N_NAME':x[1], ' N_REGIONKEY' :int(x[2]),'N_COMMENT':x[3]}).toDF( fields)
 ```


- <div dir="rtl">
  در نهایت نتیجه را بر روی hdfs  می نویسیم، برای parquet داریم:  
 </div>

```
 df.write.mode("overwrite").parquet("hdfs://namenode:8020/nation.parquet")
 ```
 

- <div dir="rtl">
   برای orc داریم:  
 </div>

```
 spark.sql("set spark.sql.orc.impl=native")
 df.write.mode("overwrite").format("orc").save("hdfs://namenode:8020/region.orc")
 ```
 
- <div dir="rtl">
   برای avro در ابندا با دستور زیر spark را اجرا کرده تا فایل مربوط به آن دانلود شود یا jar های مربوط آن را دانلود کرده و در مسیر SPARK_HOME/lib قرار دهیم سپس با دستور زیر داده ها را بر روی hdfs قرار می دهیم:   
 </div>

```
 df.write.mode("overwrite").format("com.databricks.spark.avro").save("hdfs://namenode:8020/region.avro")
 ```
<div dir="rtl">
   در نهایت می توان کوئری های تبدیل شده در OLAP/spark/query را اجرا کرد   
 </div>


### Flink 


<div dir="rtl">
   برای پیاده‌سازی این قسمت از یک پرژه تحت maven استفاده می کنیم و زبان پیاده سازی این قسمت java می‌باشد.   
 </div>
<div dir="rtl">
   برای این قسمت کافیست در ابتدا پروژه در آدرس OLAP/flink باز کرده و سپس زیر را اجرا کنیم:   
 </div>
 
```
 mvn clean install
 ```
 
<div dir="rtl">
   پس از اجرای این دستور:   
 </div>
 
- <div dir="rtl">
   پس از اجرای این دستور فایل jar مربوط به پروژه ساخته شده و در مسیر /target قرار می گیرد.   
 </div>
 
- <div dir="rtl">
   فایل jar ساخته شده را بر روی سرور قرار می دهیم   
 </div>
 

- <div dir="rtl">
   در مرحله بعد فایل های jar مربوط به flink که توسط maven دانلود شده از فولدر USER_HOME/.m2برمیداریم و بر روی سرور می بریم و در مسیر FLINK_HOME/lib قرار می دهیم   
 </div>
 
- <div dir="rtl">
   با اجرای دستور flink run –c main.Master flink-1.jar کلاس Master اجرا می شود و از کاربر شماره کوئری هایی که می خواهد اجرا شود را می پرسد، شماره هر یک از کوئری را با space جدا کرده و enter را می زنیم و flink محاسبه را شروع می کند، نتیجه لاگ flink در /flink-result.txt قرار می گیرد.   
 </div>

##OLTP
<div dir="rtl">
   در این قسمت از ابزار آماده به نام OLTPbench استفاده می کنیم، که این ابزار خود هم داده ها را در دیتابیس load کرده و هم تراکنش های مربوطه را نیز بر روی آن اعمال می کند.   
 </div>
 
<div dir="rtl">
برای این قسمت برای postgreSQL می توانید به لینک زیر مراجعه کنید:   
 </div>
 
[OLTPbench](https://github.com/oltpbenchmark/oltpbench)
 
<div dir="rtl">
و برای cockroachDB می توانید به لینک زیر مراجعه کرده و از شاخه cockroachdb آن استفاده کنید:   
 </div>
 
 [bert-s-lee OLTPbench](https://github.com/robert-s-lee/oltpbench.git)
