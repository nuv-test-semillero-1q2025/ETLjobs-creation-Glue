import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col

# Recuperar parámetros del trabajo (por ejemplo, ubicaciones de S3)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir', 'input_bucket', 'output_bucket'])

# Crear contexto de Spark y Glue
sc = SparkContext()
glueContext = GlueContext(sc)
sqlContext = SQLContext(sc)

# Definir los clientes de AWS (si se necesitan para otros servicios)
s3_client = boto3.client('s3')

# Leer el archivo CSV desde el bucket de entrada
input_path = f"s3://{args['input_bucket']}/input_file.csv"
df = sqlContext.read.format("csv").option("header", "true").load(input_path)

# Realizar una transformación simple: Convertir todas las columnas a mayúsculas
df_transformed = df.select([col(c).alias(c.upper()) for c in df.columns])

# Escribir el DataFrame transformado en formato Parquet en el bucket de salida
output_path = f"s3://{args['output_bucket']}/data/output_file.parquet"
df_transformed.write.parquet(output_path)

# Imprimir el esquema del DataFrame transformado (opcional)
df_transformed.printSchema()

# Finalizar el trabajo
job.commit()

