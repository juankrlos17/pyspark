# Autor:  Juan Sifontes
#
# Codigo pyspark que lee datos de un csv  y escribe en un topico kafka 

from pyspark.sql import SparkSession


 # TODO: Ensure this is correct
scala_version = '2.12' 
spark_version = '3.4.1'
csv_path = 's3a://buckets3/spark-test/path/data.csv'
kafka_broker = 'kafkabroker1:9093, kafkabroker2:9093, kafkabroker3:9093'
kafka_user = 'user'
kafka_pass = 'passw'
kafka_topic = 'test-spark-topic'
truststore_path = '/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks'
truststore_pass = 'XGYfrg376KH87dtPHb27Em700d'



packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.0'
]
spark = SparkSession.builder\
   .master("local")\
   .appName("certification-spark-kafka")\
   .config("spark.jars.packages", ",".join(packages))\
   .getOrCreate()

###########################

df = spark.read.text(csv_path)

############################

# Write to kafka
df_transacciones.write.format("kafka")\
  .option("kafka.bootstrap.servers", kafka_broker)\
  .option("topic", kafka_topic)\
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.ssl.truststore.location", truststore_path) \
  .option("kafka.ssl.truststore.password", truststore_pass) \
  .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_user}" password="{kafka_pass}";') \
  .save()
  
