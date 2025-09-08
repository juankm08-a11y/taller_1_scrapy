from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as pyspark_sum, avg
import os 
import shutil 

spark = SparkSession.builder.appName("ETL_Superstore").getOrCreate()

df = spark.read.csv("C:/taller_1_scrapy/ejercicio_pyspark/ETL_Superstore/Sample - Superstore.csv",header=True,inferSchema=True)

df.show(5)

df.printSchema()

df = df.withColumn("Sales",col("Sales").cast("double"))
df = df.withColumn("Profit",col("Profit").cast("double"))

df = df.withColumn("Order Date",to_date(col("Order Date"),"MM/dd/yy"))

ventas_categoria = df.groupBy("Category").agg(pyspark_sum("Sales").alias("Total Sales"))

utilidad_region = df.groupBy("Region").agg(avg("Profit").alias("Avg_Profit"))

gasto_cliente = df.groupBy("Customer ID","Customer Name").agg(pyspark_sum("Sales").alias("Total_Spent")).orderBy(col("Total_Spent").desc()).limit(1)

def save_single_csv(df,final_path):
    temp_path = final_path + "_temp"
    
    df.coalesce(1).write.csv(temp_path,header=True,mode="overwrite")
    
    for file_name in os.listdir(temp_path):
        if file_name.startswith("part-") and file_name.endswith(".csv"):
            shutil.move(os.path.join(temp_path,file_name),final_path)
        
    shutil.rmtree(temp_path)
    
if not os.path.exists("out"):
    os.makedirs("out")

save_single_csv(ventas_categoria,"out/ventas_categoria.csv")
save_single_csv(ventas_categoria,"out/utilidad_region.csv")
save_single_csv(ventas_categoria,"out/gasto_cliente.csv")
