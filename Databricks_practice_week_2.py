# Databricks notebook source


# COMMAND ----------

# create delta lake

# COMMAND ----------

df = spark.read.format("csv") .option("header", "true") .option("inferSchema", "true") .load("/Volumes/databricks_practice/default/databricks-practice2/liver_patient_dataset.csv")
display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

df = df.withColumnRenamed("A/G Ratio", "AG_Ratio")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS healthcare_db;

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("healthcare_db.patient_bronze_delta")

# COMMAND ----------

df = spark.table("healthcare_db.patient_bronze_delta")

display(df)

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load('abfss://increement-d@agilisium1.dfs.core.windows.net/')

display(df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("ClaimID")

df2 = df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

# COMMAND ----------

total_rows=df.count()

# COMMAND ----------

print(total_rows)

# COMMAND ----------

batch_size=25

# COMMAND ----------

from pyspark.sql.functions import row_number

# COMMAND ----------

for start in range(1, total_rows + 1, batch_size):

    end = start + batch_size - 1

    batch_df = df2.filter(
        (df2.row_num >= start) &
        (df2.row_num <= end)
    )

batch_df.show()
    

# COMMAND ----------

df_beneficiary = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://increement-d@agilisium1.dfs.core.windows.net/Test_Beneficiarydata-1542969243754.csv")


display(df_beneficiary)

# COMMAND ----------

df_inpatient = spark.read.format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("abfss://increement-d@agilisium1.dfs.core.windows.net/Test_Inpatientdata-1542969243754.csv")

display(df_inpatient.limit(5))

# COMMAND ----------

print(df_inpatient.columns)

# COMMAND ----------

print(df_beneficiary.columns)

# COMMAND ----------

# JOINS
# 1)only patients who were hospitalized and made inpatient insurance claims are included.

# COMMAND ----------

impaietion_claim=df_inpatient.join(df_beneficiary,on=df_inpatient.BeneID==df_beneficiary.BeneID,how='inner')
display(impaietion_claim)

# COMMAND ----------

df.claims_list=df_inpatient.join(df_beneficiary,on=df_inpatient.BeneID==df_beneficiary.BeneID,how='left')
df.claims_list.display()

# COMMAND ----------

# Find patients with highest reimbursement claims.

# COMMAND ----------

high_rereimbursement=df_inpatient.join(df_beneficiary,on=df_inpatient.BeneID==df_beneficiary.BeneID,how="inner")

# COMMAND ----------

high_rereimbursement.display(high_rereimbursement.limit(5))

# COMMAND ----------

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://increement-d@agilisium1.dfs.core.windows.net/Test_Inpatientdata-1542969243754.csv")
df.display(df.limit(5))
df_inpatient = spark.read.format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("abfss://increement-d@agilisium1.dfs.core.windows.net/Test_Inpatientdata-1542969243754.csv")
df_beneficiary = spark.read.format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("abfss://increement-d@agilisium1.dfs.core.windows.net/Test_Beneficiarydata-1542969243754.csv")
df_inpatient = spark.read.format("csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .load("abfss://increement-d@agilisium1.dfs.core.windows.net/Test_Inpatientdata-1542969243754.csv")

# COMMAND ----------

# window funtictions ,aggreations oprations 

# COMMAND ----------

from pyspark.sql.functions import col#  im trying to Find highest claim for each beneficiary.


window_spec = Window.partitionBy("BeneID") \
                    .orderBy(col("InscClaimAmtReimbursed").desc())

silver_df =df_inpatient.withColumn(
    "claim_rank",
    row_number().over(window_spec)
)
silver_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS practice;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS practice_customer_silver
# MAGIC

# COMMAND ----------

data = [
    (1, "Atish", "Pune"),
    (2, "Rahul", "Mumbai"),
    (3, "Amit", "Delhi")
]

df4 = spark.createDataFrame(data, ["id", "name", "city"])

display(df4)

# COMMAND ----------

df4.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("practice.customer")

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("practice_customer_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM practice.customer;

# COMMAND ----------

# MAGIC %sql SHOW CATALOGS

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_practice.practice;
# MAGIC

# COMMAND ----------

data = [
    (1, "Atish", "Pune"),
    (2, "Rahul", "Mumbai"),
    (3, "Amit", "Delhi")
]

df = spark.createDataFrame(
    data,
    ["id", "name", "city"]
)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "databricks_practice.practice.employee"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_practice.practice.employee;

# COMMAND ----------

source_data = [
    (2, "Rahul", "Nagpur"),
    (4, "Vijay", "Chennai")
]

source_df = spark.createDataFrame(
    source_data,
    ["id","name","city"]
)

source_df.createOrReplaceTempView("source_customer")

# COMMAND ----------

# MAGIC %sql SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN databricks_practice.practice;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN databricks_practice;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO databricks_practice.practice.employee AS tgt
# MAGIC
# MAGIC USING source_customer AS src
# MAGIC
# MAGIC ON tgt.id = src.id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC     tgt.name = src.name,
# MAGIC     tgt.city = src.city
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (id, name, city)
# MAGIC VALUES (
# MAGIC     src.id,
# MAGIC     src.name,
# MAGIC     src.city
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_practice.practice.employee;

# COMMAND ----------

cdc_data = [
    ("U",1,"Atish","Nashik"),
    ("D",2,"Rahul","Nagpur"),
    ("I",5,"Suresh","Pune")
]

cdc_df = spark.createDataFrame(
    cdc_data,
    ["op","id","name","city"]
)

cdc_df.createOrReplaceTempView("cdc_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cdc_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO databricks_practice.practice.employee AS tgt
# MAGIC
# MAGIC USING (
# MAGIC     SELECT *
# MAGIC     FROM cdc_customer
# MAGIC     WHERE op = 'U'
# MAGIC ) AS src
# MAGIC
# MAGIC ON tgt.id = src.id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC     tgt.name = src.name,
# MAGIC     tgt.city = src.city;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM databricks_practice.practice.employee
# MAGIC WHERE id IN (
# MAGIC     SELECT id
# MAGIC     FROM cdc_customer
# MAGIC     WHERE op = 'D'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO databricks_practice.practice.employee
# MAGIC SELECT
# MAGIC     id,
# MAGIC     name,
# MAGIC     city
# MAGIC FROM cdc_customer
# MAGIC WHERE op = 'I';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_practice.practice.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE
# MAGIC databricks_practice.practice.employee_scd2
# MAGIC (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     city STRING,
# MAGIC     start_date DATE,
# MAGIC     end_date DATE,
# MAGIC     is_current STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO databricks_practice.practice.employee_scd2
# MAGIC VALUES
# MAGIC (
# MAGIC     1,
# MAGIC     'Atish',
# MAGIC     'Pune',
# MAGIC     current_date(),
# MAGIC     DATE('9999-12-31'),
# MAGIC     'Y'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE databricks_practice.practice.employee_scd2
# MAGIC
# MAGIC SET
# MAGIC     end_date = current_date(),
# MAGIC     is_current = 'N'
# MAGIC
# MAGIC WHERE id = 1
# MAGIC AND is_current = 'Y';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC databricks_practice.practice.employee_scd2
# MAGIC
# MAGIC VALUES
# MAGIC (
# MAGIC 1,
# MAGIC 'Atish',
# MAGIC 'Nashik',
# MAGIC current_date(),
# MAGIC DATE('9999-12-31'),
# MAGIC 'Y'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_practice.practice.employee_scd2
# MAGIC ORDER BY id,start_date;

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY databricks_practice.practice.employee_scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_practice.practice.employee_scd2
# MAGIC VERSION AS OF 1;

# COMMAND ----------

spark.sql("OPTIMIZE databricks_practice.practice.employee_scd2")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE databricks_practice.practice.employee_scd2;

# COMMAND ----------

# Time Travel, Optimize, Vacuum

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO databricks_practice.practice.employee_scd2
# MAGIC VALUES
# MAGIC (
# MAGIC     6,
# MAGIC     'Ramesh',
# MAGIC     'Hyderabad',
# MAGIC     current_date(),
# MAGIC     DATE('9999-12-31'),
# MAGIC     'Y'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_practice.practice.employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY databricks_practice.practice.employee_scd2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_practice.practice.employee
# MAGIC VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_practice.practice.employee
# MAGIC VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE databricks_practice.practice.employee
# MAGIC ZORDER BY (id);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM databricks_practice.practice.employee
# MAGIC WHERE id = 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM databricks_practice.practice.employee DRY RUN;

# COMMAND ----------

#BRONZE
#SILVER
#GOLD

# COMMAND ----------

#remove dublicates
df_inpatient_clean = (
    df_inpatient
    .dropDuplicates(["ClaimID"])
)

# COMMAND ----------

#null handles 
from pyspark.sql.functions import col
df_inpatient_clean = (
    df_inpatient_clean
    .filter(col("ClaimID").isNotNull())
)

# COMMAND ----------

silver_df = (
    df_inpatient_clean.alias("i")
    .join(
        df_beneficiary.alias("b"),
        "BeneID",
        "left"
    )
)

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df = (
    df_inpatient_clean.alias("i")
    .join(
        df_beneficiary.alias("b"),
        "BeneID",
        "left"
    )
)

# COMMAND ----------

silver_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN databricks_practice;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_practice.healthcare;

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "databricks_practice.practice.silver_claims"
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_practice.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_practice.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_practice.gold;

# COMMAND ----------

#Gold layer oprations
# State-wise Claims

# COMMAND ----------

from pyspark.sql.functions import sum,count
gold_state = (silver_df.groupBy("State").agg(sum("InscClaimAmtReimbursed").alias("Total_Claim_Amount"),count
("ClaimID").alias("Total_Claims")))

# COMMAND ----------

gold_state.display()

# COMMAND ----------

# find total count by the gender
gold_gender = (silver_df.groupBy("Gender").count())

# COMMAND ----------

gold_gender.display()

# COMMAND ----------

#Average Claim Amount
gold_avg_claim = (silver_df.groupBy("State").avg("InscClaimAmtReimbursed"))

# COMMAND ----------

#im saving whole data in gold layer 
gold_state.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
"databricks_practice.healthcare.gold_state_claims"
)

# COMMAND ----------

from pyspark.sql.functions import avg

gold_avg_claim = (
    silver_df
    .groupBy("State")
    .agg(
        avg("InscClaimAmtReimbursed").alias("Avg_Claim_Amount")
    )
)

# COMMAND ----------

# State-wise Claims
#Gender-wise Patient Count
#Average Claim Amount by State
gold_state.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "databricks_practice.healthcare.gold_state_claims"
    )
gold_gender.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "databricks_practice.healthcare.gold_gender_claims"
    ) 
gold_avg_claim.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "databricks_practice.healthcare.gold_avg_claims"
    )

# COMMAND ----------

print(gold_state.columns)
print(gold_gender.columns)
print(gold_avg_claim.columns)

# COMMAND ----------

snowflakeOptions = {

    "sfURL": "XJYKRXE-FC82624.snowflakecomputing.com",

    "sfUser": "ATISHDHEKNE",

    "sfPassword": "Atish@9765384860", 

    "sfDatabase": "HEALTHCARE_DB",

    "sfSchema": "GOLD",

    "sfWarehouse": "COMPUTE_WH"

}

# COMMAND ----------

snowflakeOptions = {
    "sfUrl": "XJYKRXE-FC82624.snowflakecomputing.com",
    "sfUser": "ATISHDHEKNE",
    "sfPassword": "Atish@9765384860",
    "sfDatabase": "HEALTHCARE_DB",
    "sfSchema": "GOLD",
    "sfWarehouse": "COMPUTE_WH"
}

# COMMAND ----------

test_df = spark.createDataFrame(
    [(1, "Atish")],
    ["id", "name"]
)

# COMMAND ----------

gold_state = spark.table(
    "databrickspractice2.healthcare.gold_state_claims"
)

# COMMAND ----------

gold_state = spark.table(
    "databricks_practice.healthcare.gold_state_claims"
)

gold_state.write \
    .format("snowflake") \
    .options(**snowflakeOptions) \
    .option("dbtable", "GOLD_STATE_CLAIMS") \
    .mode("overwrite") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC Succusefully Loaded data into Snowflake

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS healthcare_audit;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS healthcare_audit.job_audit_log (
# MAGIC     job_name STRING,
# MAGIC     task_name STRING,
# MAGIC     status STRING,
# MAGIC     start_time TIMESTAMP,
# MAGIC     end_time TIMESTAMP,
# MAGIC     duration_seconds DOUBLE,
# MAGIC     error_message STRING,
# MAGIC     run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from datetime import datetime
dbutils.widgets.text("run_id", "manual_run")
start_time = datetime.now()

job_name = "Healthcare_Workflow"
task_name = "ingestion_task"
status = "SUCCESS"
error_message = None
run_id = dbutils.widgets.get("run_id") if dbutils.widgets.get("run_id") else "manual_run"

try:
    # YOUR ETL CODE HERE
    print("Running ETL logic...")

    status = "SUCCESS"

except Exception as e:
    status = "FAILED"
    error_message = str(e)

finally:
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    spark.sql(f"""
        INSERT INTO healthcare_audit.job_audit_log VALUES (
            '{job_name}',
            '{task_name}',
            '{status}',
            TIMESTAMP('{start_time}'),
            TIMESTAMP('{end_time}'),
            {duration},
            '{error_message}',
            '{run_id}'
        )
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM healthcare_audit.job_audit_log
# MAGIC ORDER BY start_time DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC UNITY CATLOG

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC GRANT SELECT ON TABLE databricks_practice.healthcare.gold_avg_claims TO "atishdhekne8@gmail.com";

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW FUNCTIONS IN default;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN databricks_practice;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION databricks_practice.healthcare.mask_amount(x DOUBLE)
# MAGIC RETURN CASE
# MAGIC   WHEN is_member('atishdhekne8@gmail.com') THEN x
# MAGIC   ELSE NULL
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC Git Integration, CI/CD
# MAGIC
