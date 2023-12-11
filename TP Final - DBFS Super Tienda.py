# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## SuperTienda
# MAGIC
# MAGIC Análisis del promedio de ventas

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Supertienda.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Especifica la ruta del archivo
ruta_archivo = "/FileStore/tables/Supertienda.csv"

# Lee el archivo CSV en un DataFrame de Spark, saltando la primera fila
df = spark.read.option("header", "true").csv(ruta_archivo)

# Muestra las primeras filas del DataFrame
df.show(10)


# COMMAND ----------

# Crear una vista temporal
df.createOrReplaceTempView("mi_tabla")

# Ejecutar la consulta SQL
resultado_sql = spark.sql("SELECT Category, AVG(CAST(Sales AS DOUBLE)) as AvgSales FROM mi_tabla GROUP BY Category")
display(resultado_sql)


# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "Supertienda_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# Mostrar las primeras filas del DataFrame
df.show(10)

# COMMAND ----------

# Ejemplo de visualización
display(df)

# Ejemplo de preprocesamiento (eliminar columnas no deseadas)
columnas_no_deseadas = ["Row ID", "Order ID", "Ship Date"]
df = df.drop(*columnas_no_deseadas)

# COMMAND ----------

# Ejemplo de SQL en Databricks
df.createOrReplaceTempView("mi_tabla")
resultado_sql = spark.sql("SELECT Category, AVG(Sales) as AvgSales FROM mi_tabla GROUP BY Category")
display(resultado_sql)

# COMMAND ----------

import matplotlib.pyplot as plt

# Convertir los resultados a un DataFrame de pandas para la visualización
df_resultados = resultado_sql.toPandas()

# Gráfico de barras
plt.bar(df_resultados['Category'], df_resultados['AvgSales'])
plt.xlabel('Category')
plt.ylabel('AvgSales')
plt.title('Promedio de Ventas por Categoría')
plt.show()


# COMMAND ----------

resultado_sql_desglosado = spark.sql("SELECT Category, Region, AVG(CAST(Sales AS DOUBLE)) as AvgSales FROM mi_tabla GROUP BY Category, Region")
display(resultado_sql_desglosado)

# COMMAND ----------

# Crear una vista temporal
df.createOrReplaceTempView("mi_tabla")

# Ejecutar la consulta SQL para el desglose por categoría y región
resultado_sql_desglosado = spark.sql("""
    SELECT Category, Region, AVG(CAST(Sales AS DOUBLE)) as AvgSales
    FROM mi_tabla
    GROUP BY Category, Region
""")

# Mostrar los resultados desglosados
display(resultado_sql_desglosado)


# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Convertir los resultados desglosados a un DataFrame de pandas para la visualización
df_desglosado = resultado_sql_desglosado.toPandas()

# Crear un gráfico de barras apiladas
pivot_df = df_desglosado.pivot(index='Category', columns='Region', values='AvgSales')
pivot_df.plot(kind='bar', stacked=True, figsize=(10, 6))

# Personalizar el gráfico
plt.xlabel('Category')
plt.ylabel('AvgSales')
plt.title('Promedio de Ventas por Categoría y Región')
plt.legend(title='Region', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()


"""

Analizando los gráficos que generamos:

Promedio de Ventas por Categoría:
Análisis:
En el primer gráfico de barras, observamos el promedio de ventas por categoría. Las tres categorías principales son "Office Supplies," "Furniture," y "Technology."
La categoría "Technology" tiene el promedio de ventas más alto, seguida por "Furniture" y "Office Supplies."
Este gráfico proporciona una visión general de cómo se comparan las categorías en términos de ventas promedio.
Conclusión:
La categoría "Technology" es la líder en términos de promedio de ventas, lo que sugiere que los productos tecnológicos son más populares o pueden tener un mayor valor de venta.
La categoría "Furniture" sigue en importancia, indicando que los muebles también contribuyen significativamente a las ventas.
"Office Supplies" tiene el promedio de ventas más bajo, pero sigue siendo una parte esencial del negocio.
Promedio de Ventas por Categoría y Región:
Análisis:
El segundo gráfico de barras apiladas proporciona un desglose adicional, mostrando cómo se distribuyen las ventas promedio por categoría en diferentes regiones.
Cada barra representa una categoría, y los segmentos de colores representan las regiones.
Podemos observar cómo se comparan las categorías en cada región.
Conclusión:
"Technology" tiene el promedio de ventas más alto en todas las regiones, destacando su importancia general en cada área geográfica.
La importancia relativa de las categorías puede variar según la región. Por ejemplo, en la región "South," "Furniture" contribuye significativamente, mientras que en la región "West," "Office Supplies" tiene una presencia más destacada.
Este gráfico proporciona información valiosa sobre cómo las preferencias de compra pueden variar geográficamente, lo que puede ser útil para estrategias de marketing y gestión de inventario.
"""

