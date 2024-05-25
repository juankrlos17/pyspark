from pyspark.sql import SparkSession

scala_version = '2.12'  # TODO: Ensure this is correct
spark_version = '3.4.1'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.0'
]
spark = SparkSession.builder\
   .master("local")\
   .appName("certification-axo-spark-kafka")\
   .config("spark.jars.packages", ",".join(packages))\
   .getOrCreate()

# Read all lines into a single value dataframe  with column 'value'
# TODO: Replace with real file. 
df = spark.read.text('file://data.csv')

# TODO: Remove the file header, if it exists

# Write
df.write.format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("topic", "foobar")\
  .save()
