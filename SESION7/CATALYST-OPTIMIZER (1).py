# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Parsed Logical Plan:
# MAGIC #### Definición:
# MAGIC En esta etapa, Spark interpreta y traduce la sintaxis de la consulta escrita por el usuario en una representación lógica. Es una etapa temprana donde Spark intenta entender la intención de la consulta.
# MAGIC #### Resultado:
# MAGIC Muestra la estructura lógica de la operación que el usuario solicitó, como las tablas involucradas y las condiciones de filtro o unión.
# MAGIC
# MAGIC ### 2. Analyzed Logical Plan:
# MAGIC #### Definición:
# MAGIC Después de la etapa de análisis, Spark asigna tipos de datos a las columnas y realiza otras tareas de verificación. Es una etapa donde Spark "comprende" las relaciones y los tipos de datos de las columnas.
# MAGIC #### Resultado: 
# MAGIC Muestra el plan lógico después de haber analizado y asignado tipos de datos. Las columnas están identificadas con sus tipos, lo que facilita la interpretación y ejecución subsiguiente.
# MAGIC
# MAGIC ### 3. Optimized Logical Plan:
# MAGIC
# MAGIC ####Definición: 
# MAGIC En esta etapa, Spark aplica optimizaciones al plan lógico para mejorar el rendimiento. Se pueden realizar transformaciones lógicas para simplificar la ejecución y reducir el tiempo de procesamiento.
# MAGIC #### Resultado: 
# MAGIC Muestra el plan lógico después de aplicar optimizaciones. Puede incluir cambios en el orden de las operaciones, la eliminación de operaciones redundantes y otras transformaciones para hacer la ejecución más eficiente.
# MAGIC
# MAGIC ### 4. Physical Plan:
# MAGIC
# MAGIC #### Definición:
# MAGIC En esta etapa, Spark traduce el plan lógico optimizado en un plan físico que especifica cómo se ejecutará la consulta en el clúster distribuido.
# MAGIC
# MAGIC #### Resultado:
# MAGIC Muestra la representación física de la ejecución, incluyendo las tareas específicas que se realizarán en cada nodo del clúster. Puede incluir operaciones como escaneo de archivos, operaciones de filtrado y operaciones de join.
# MAGIC ## Conclusión
# MAGIC En resumen, estas secciones reflejan el proceso de traducción y optimización que Spark realiza para ejecutar una consulta. Comienzan desde la interpretación de la consulta del usuario hasta la generación del plan físico detallado que se ejecutará en un entorno distribuido. Cada etapa es crucial para garantizar la eficiencia y el rendimiento de las consultas en Spark.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/datalake/golden/

# COMMAND ----------

# MAGIC %fs ls dbfs:/datalake/golden/TransactionsByPerson

# COMMAND ----------

transactionByPerson =  spark.read.format("parquet").load("dbfs:/datalake/golden/TransactionsByPerson")

# COMMAND ----------

display(transactionByPerson)

# COMMAND ----------

transactionByPerson.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Activación del plan Físico Otpimizado

# COMMAND ----------

from pyspark.sql.functions import col
transactionByPerson.filter(col("FECHA") == "2021-01-22").explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ACTIVANDO EL PLAN OPTIMIZADO

# COMMAND ----------

from pyspark.sql.types import IntegerType
result = transactionByPerson.filter((col("MONTO") > 2) ).filter(col("MONTO") > 2)

# COMMAND ----------

result.explain(True)

# COMMAND ----------

dataframe1 = transactionByPerson.alias("table1").filter(col("MONTO") > 200)
dataframe2 = dataframe1.filter(col("MONTO") > 200)

# COMMAND ----------

dataframe2.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimizaciones del Optimizador Catalyst en Spark SQL
# MAGIC
# MAGIC ## 1. Pushdown de Predicados
# MAGIC **Descripción**: Mueve las cláusulas de filtro lo más cerca posible a la fuente de datos.
# MAGIC
# MAGIC **Ejemplo**:
# MAGIC
# MAGIC - Consulta Original: `SELECT * FROM ventas WHERE año = 2021;`
# MAGIC - Optimización: Aplica el filtro `año = 2021` directamente en la fuente de datos.
# MAGIC
# MAGIC ## 2. Proyección de Columnas (Projection Pruning)
# MAGIC **Descripción**: Elimina las columnas no necesarias de la consulta lo antes posible.
# MAGIC
# MAGIC **Ejemplo**:
# MAGIC
# MAGIC - Consulta Original: `SELECT nombre FROM empleados;`
# MAGIC - Optimización: Solo lee y procesa la columna `nombre`, ignorando las demás.
# MAGIC
# MAGIC ## 3. Eliminación de Subconsultas Redundantes (Subquery Elimination)
# MAGIC **Descripción**: Detecta y elimina subconsultas redundantes.
# MAGIC
# MAGIC **Ejemplo**:
# MAGIC
# MAGIC - Consulta Original: `SELECT AVG(salario) FROM (SELECT salario FROM empleados) AS subconsulta;`
# MAGIC - Optimización: Elimina la subconsulta, usando `SELECT AVG(salario) FROM empleados;`.
# MAGIC
# MAGIC ## 4. Reordenamiento de Join (Join Reordering)
# MAGIC **Descripción**: Cambia el orden de las operaciones de join para minimizar el coste computacional.
# MAGIC
# MAGIC **Ejemplo**:
# MAGIC
# MAGIC - Consulta Original: `SELECT * FROM tabla1 JOIN tabla2 ON tabla1.id = tabla2.id JOIN tabla3 ON tabla2.id = tabla3.id;`
# MAGIC - Optimización: Reordena los joins para reducir la cantidad de datos procesados.
# MAGIC
# MAGIC ## 5. Optimización de Agregaciones (Aggregate Optimization)
# MAGIC **Descripción**: Mejora el rendimiento de las operaciones de agregación.
# MAGIC
# MAGIC **Ejemplo**:
# MAGIC
# MAGIC - Consulta Original: `SELECT COUNT(*), AVG(salario) FROM empleados;`
# MAGIC - Optimización: Combina operaciones de agregación en una sola pasada.
# MAGIC
# MAGIC ## 6. Regla de Propagación de Constantes
# MAGIC **Descripción**: Resuelve operaciones o comparaciones con constantes durante la planificación.
# MAGIC
# MAGIC **Ejemplo**:
# MAGIC
# MAGIC - Consulta Optimizada: `SELECT a, b, 3 as c FROM mi_tabla WHERE true;`

# COMMAND ----------


