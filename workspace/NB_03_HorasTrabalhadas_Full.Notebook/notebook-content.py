# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "11540624-ed9f-41e1-9b0e-6bd60e915153",
# META       "default_lakehouse_name": "LK01",
# META       "default_lakehouse_workspace_id": "cb84b88f-2b8c-480e-9c0b-2e37850e3d4b"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *

# Define o schema do dataframe
csv_schema = StructType([ \
    StructField("Matricula", IntegerType(), True), \
    StructField("Data", DateType(), True), \
    StructField("HorasNormais", DoubleType(), True), \
    StructField("HorasTrabalhadas", DoubleType(), True)
  ])

# Lê o arquivo
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("dateFormat", "dd/MM/yyyy") \
    .schema(csv_schema) \
    .load("Files/horas_trabalhadas.csv")

display(df)

# Salva nas tabelas delta
df.write.mode('overwrite').saveAsTable("staging_horas_trabalhadas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Apaga a tabela se existir
# MAGIC DROP TABLE IF EXISTS tb_horas_trabalhadas;
# MAGIC 
# MAGIC -- Cria a tabela a partir incluindo a 'MatriculaSk' da tabela 'tb_funcionarios'
# MAGIC CREATE TABLE tb_horas_trabalhadas AS
# MAGIC SELECT 
# MAGIC     h.Matricula, 
# MAGIC     h.Data, 
# MAGIC     h.HorasNormais, 
# MAGIC     h.HorasTrabalhadas,
# MAGIC     f.MatriculaSk
# MAGIC FROM 
# MAGIC     staging_horas_trabalhadas h
# MAGIC 
# MAGIC LEFT JOIN tb_funcionarios f
# MAGIC     ON h.Matricula = f.Matricula
# MAGIC     AND h.Data BETWEEN f.DataVigenciaInicial AND f.DataVigenciaFinal;
# MAGIC 
# MAGIC SELECT * FROM tb_horas_trabalhadas;
# MAGIC 
# MAGIC -- Apaga a tabela temporária
# MAGIC DROP TABLE IF EXISTS staging_horas_trabalhadas;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
