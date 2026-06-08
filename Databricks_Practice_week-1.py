# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/Volumes/practice_of_databricks/default/helthcare_data/Test_Beneficiarydata-1542969243754.csv")

# COMMAND ----------

# DBTITLE 1,ion
df2=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/Volumes/practice_of_databricks/default/helthcare_data/Train_Outpatientdata-1542865627584.csv")

# COMMAND ----------

df3=spark.read.format("csv").option("header","True").option("inferschema","True").load("/Volumes/practice_of_databricks/default/helthcare_data/Train_Inpatientdata-1542865627584.csv")

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 1)select
# MAGIC   2) filter
# MAGIC   3) withColumn
# MAGIC   4)cjoin
# MAGIC   5) groupBy
# MAGIC   6) agg
# MAGIC   7)romoveDuplicates
# MAGIC   8) orderBy
# MAGIC   9)union
# MAGIC   10) explode
# MAGIC   11)split
# MAGIC   12) regexp_replace
# MAGIC   13) pivot
# MAGIC   14)cache
# MAGIC   15)repartition

# COMMAND ----------

display(df)

# COMMAND ----------

# select
display(df.select(col("gender")).limit(5))

# COMMAND ----------

display(df.filter(df.Gender=="1").limit(5))

# COMMAND ----------

display(df2.limit(5))

# COMMAND ----------

display(df3.limit(5))

# COMMAND ----------

# for finding primary key
display(df3.groupBy("ClaimID").count().filter("count > 1"))

# COMMAND ----------

# withColumn

# ✔️ add new col
# ✔️add contant col
# ✔️add contant col
# ✔️modify existing col
# ✔️drop col
# icoditional col
#mathematical oprations
#type casting


# COMMAND ----------

df2 = df2.withColumn("settel_calaims",rand())

# COMMAND ----------

display(df2.limit(5))

# COMMAND ----------

df2.display(df2=df2.withColumn("unsettels_calaims",lit("131231")))

# COMMAND ----------

#modify existing col
df2.display(df2=df2.withColumn("settel_claims",df2["InscClaimAmtReimbursed"]*2))

# COMMAND ----------

#drop column
df2.display(df2=df2.drop("DeductibleAmtPaid"))

# COMMAND ----------

df2=df2.withColumn

# COMMAND ----------



# COMMAND ----------

# performance optimazatins 
# 1) partitions on patient data 
# 2) cache
# 3) dignosis 
