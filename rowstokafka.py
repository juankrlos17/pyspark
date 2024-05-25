from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import to_json, struct, col
from random import randint, choice
import datetime

 # TODO: Ensure this is correct
scala_version = '2.12' 
spark_version = '3.4.1'
kafka_broker = 'axo-kafka-test-corebroker2.jsifonte.a465-9q4k.cloudera.site:9093, axo-kafka-test-corebroker1.jsifonte.a465-9q4k.cloudera.site:9093, axo-kafka-test-corebroker0.jsifonte.a465-9q4k.cloudera.site:9093'
kafka_user = 'user'
kafka_pass = 'xxxxxxx'
kafka_topic = 'test-spark'
truststore_path = '/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_truststore.jks'
truststore_pass = 'XGYfrg376KH87dtPHb27Em700d'



packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.0'
]
spark = SparkSession.builder\
   .master("local")\
   .appName("certification-axo-spark-kafka")\
   .config("spark.jars.packages", ",".join(packages))\
   .getOrCreate()



###########################
# Generar registros aleatorios
# Esquema para el DataFrame de transacciones
schema = StructType([
    StructField("id_transaccion", StringType(), nullable=False),
    StructField("producto", StringType(), nullable=False),
    StructField("cantidad", FloatType(), nullable=False),
    StructField("monto", FloatType(), nullable=False),
    StructField("fecha", TimestampType(), nullable=False)
])

# Función para generar registros aleatorios de transacciones
def generar_transaccion():
    productos = ["producto1", "producto2", "producto3", "producto4", "producto5"]
    id_transaccion = randint(1, 1000)
    producto = choice(productos)
    cantidad = float(randint(1, 10))
    monto = round(randint(10, 1000) * cantidad, 2)
    fecha = datetime.datetime.now()
    return id_transaccion, producto, cantidad, monto, fecha

# Generar datos aleatorios y crear un RDD de transacciones
rdd_transacciones = spark.sparkContext.parallelize([generar_transaccion() for _ in range(100)])

# Crear DataFrame a partir del RDD y aplicar el esquema
df_transacciones = spark.createDataFrame(rdd_transacciones, schema)

df_transacciones = df_transacciones.select(col("id_transaccion").alias("key"), to_json(struct([col(c) for c in df_transacciones.columns])).alias("value"))

# Mostrar el DataFrame con las transacciones generadas
df_transacciones.show(truncate=False)

#############################


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

