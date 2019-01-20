# TPC-benchmark
<div dir="rtl">
هدف از انجام این پروژه مقایسه کارایی دیتابیس های مختلف در دو حوزه OLAP و OLTP می باشد، برای انجام این کار از دیتابیس های postgreSQL ، spark (با فرمت های parquet ، orc و  avro برای ذخیره سازی) و flink (تنها با فرمت avro برای ذخیره سازی) در حوزه OLAP و از دیتابیس های postgreSQL و cockroachDB در حوزه OLTP استفاده شده است.
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
