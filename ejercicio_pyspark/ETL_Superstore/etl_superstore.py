from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum

spark = SparkSession.builder \
    .appName("ETL_Superstore") \
    .getOrCreate()
    
df = spark.read.csv("Superstore.csv",header=True,inferSchema=True)

df.show(5)

df.printSchema()

df = df.withColumn("Sales",col("Sales").cast("double"))
df = df.withColumn("Profit",col("Profit").cast("double"))
df = df.withColumn("Order Date",to_date(col("Order Date"),"MM/dd/yyyy"))

ventas_categoria = df.groupBy("Category").sum("Sales").withColumnRenamed("sum(Sales)","Total_Sales")
ventas_categoria.show()

utilidad_region = df.groupBy("Region").avg("Profit").withColumnRenamed("avg(Profit)","Avg_Profit")
utilidad_region.show()

gasto_cliente = df.groupBy("Customer Name") \
    .agg(_sum("Sales").alias("Total_Spend")) \
    .orderBy(col("Total_Spend").desc()) \
        .limit(1)

gasto_cliente.show()

ventas_categoria.write.csv("out/ventas_categoria.csv",header=True,mode="overwrite")
ventas_categoria.write.csv("out/utilidad_region.csv",header=True,mode="overwrite")
ventas_categoria.write.csv("out/gasto_cliente.csv",header=True,mode="overwrite")
