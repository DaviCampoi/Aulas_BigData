from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as F_round

spark = SparkSession.builder.appName("ExemploPySparkSimples").getOrCreate()

print("==============================================")
print("SparkSession inicializada com sucesso!")
print("==============================================")

dados = [
    ("Alice", "Vendas", 30, 3000.00),
    ("Bob", "TI", 35, 4500.00),
    ("Charlie", "Vendas", 25, 3200.00),
    ("David", "TI", 40, 5000.00),
    ("Eve", "RH", 28, 3800.00),
    ("Frank", "Vendas", 32, 3000.00)
]
colunas = ["Nome", "Departamento", "Idade", "Salario"]

df = spark.createDataFrame(data=dados, schema=colunas)

print("\n--- 1. DataFrame Original ---")
df.show()
df.printSchema()

df_filtrado = df.filter(col("Salario") > 3500) \
                .withColumn("Bonus", F_round(col("Salario") * 0.10, 2))

print("\n--- 2. DataFrame Filtrado (Salário > 3500) com Coluna 'Bonus' ---")
df_filtrado.show()

df_agregado = (
    df.groupBy("Departamento")
      .agg(F_round(avg("Salario"), 2).alias("Salario_Medio"))
      .orderBy(col("Salario_Medio").desc())
)

print("\n--- 3. Salário Médio por Departamento ---")
df_agregado.show()

spark.stop()
print("==============================================")
print("Processamento PySpark concluído e SparkSession encerrada.")
print("==============================================")
