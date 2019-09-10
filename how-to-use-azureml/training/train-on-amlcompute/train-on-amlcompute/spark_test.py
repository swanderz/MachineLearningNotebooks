import findspark as fs
import pyspark

print("find spark")
fs.find()
print("init spark")
fs.init()
print("Connect to Spark")
# Start Spark session
spark = pyspark.sql.SparkSession.builder.appName('Attrition').getOrCreate()
print("spark con", spark)