(Toan) Dùng aggregate function như tính tổng, tính trung bình ```(Shuffle, sort)``` (Hiểu trên ``SparkUI``)

(Toan) Bai toan de join 2, 3, 4 tap data lai voi nhau (Hiểu dc SparkUI, query execution cho các bài dưới)

Dung` tập data answer join 2,3,4 lần để lấy thêm field.

ví dụ tập a lấy id, title. tập b lấy body. tập c lấy asnwercount etc.

Hiểu kiểu join bt và kiểu join broadcast (in memory) trong Spark

(Tung Pham, Toan) Khi write parquet ra output, hiểu bản chất khi dùng 2 hàm: ``repartition()`` vs ``coalesce()`` 

Delta Lake releases versions: https://docs.delta.io/latest/releases.html

Delta Lake v3.2.0: https://github.com/delta-io/delta/releases/tag/v3.2.0


Submit 1 spark job tren dataproc -> collect cac metrics cuar spark job nhu: stage, pass/failed.
Lo 1 spark job co nhieu stages thi khi no failed o stage nao thi can biet stage do.

Implement SparkListener roi submit 1 spark job tren dataproc xem co lay duoc thong tin metadata cua spark ko

apache spark job monitoring

Delta lake: upset, merge

ETL: 

Data warehouse limitations:
- Long ETL process
- Can't process real time data
- Can store only structure data
- Not support for streaming data

Delta Lake
- Structured
- Unstructured
- Semi structured data
- Limitations:
  - Difficult to enforce data governance
  - Data quality issue
  - Data duplication
  - Difficult to find data
- One of the key features of Delta Lake is the asset transaction support

Four components:
- Delta Files: Parquet file
- Delta Tables: Transaction Log
- Delta Storage Layer: Keep data in Object Storage
- Delta Engine: It is available only in Databricks, it can process data faster than the spark engine

Delta Table:
  - ACID transaction support
  - Delta Log
  - 


Spark Concurrent Jobs:
- https://towardsdatascience.com/apache-spark-sharing-fairly-between-concurrent-jobs-d1caba6e77c2
- 
