# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ce627731-9b73-4ac7-8826-348580796c17",
# META       "default_lakehouse_name": "PMB_Data_Dev",
# META       "default_lakehouse_workspace_id": "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e",
# META       "known_lakehouses": [
# META         {
# META           "id": "ce627731-9b73-4ac7-8826-348580796c17"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import regexp_replace, trim,when,col

df = spark.read.table("VFAExtract")

df = df.withColumnRenamed("Asset - Portfolio Name", "Portfolio_Name") \
       .withColumnRenamed("Asset - Asset Name", "Property_Name") \
       .withColumnRenamed("Asset - Number", "Building_ID") \
       .withColumnRenamed("Asset - Building Priority Detail", "Building_Priority_Detail") \
       .withColumnRenamed("Asset - Use", "Use_Type") \
       .withColumnRenamed("Asset - Trailer/Portable Use", "Trailer_Portable_Use") \
       .withColumnRenamed("Asset - FCI", "FCI") \
       .withColumnRenamed("Requirement - Linked System", "Linked_System") \
       .withColumnRenamed("Requirement - Category", "Category") \
       .withColumnRenamed("Requirement - Requirement Name", "Requirement_Name") \
       .withColumnRenamed("Requirement - Estimated Cost", "EstimatedCost") \
       .withColumnRenamed("Requirement - Priority", "Priority") \
       .withColumnRenamed("Requirement - Impact", "Impact") \
       .withColumnRenamed("Requirement - RAP Project ID", "RAPProjectID") \
       .withColumnRenamed("Requirement - ID", "Requirement_EID") \
       .withColumnRenamed("Asset - City", "City") \
       .withColumnRenamed("Requirement - System Group", "System_Group") \
       .withColumnRenamed("Requirement - RAP Project Status", "RAPProjectStatus") \
       .withColumnRenamed("Requirement - Ranking #2", "RankingTwo") \
       .withColumnRenamed("Requirement - Renewal", "IsRenewal") \
       .withColumnRenamed("Requirement - Action FY", "Action_FY")

df = df.withColumn("Uniformat", trim(regexp_replace(col("Linked_System"), "-.*", "")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

### Impact 
from pyspark.sql.functions import when, col
from pyspark.sql.functions import split, col




df = df.withColumn(
    "Estimated_cost_missing",
    when(col("EstimatedCost").isNull() | (col("EstimatedCost") < 0), "1").otherwise("")
)



df = df.withColumn(
    "Impact_missing",
    when(col("Impact").isNull(),"1").otherwise("")
)

### Linked System

df = df.withColumn(
    "LinkedSystem_missing",
    when(col("Linked_System").isNull(),"1").otherwise("")
)

### System Group 

df = df.withColumn(
    "SystemGroup_missing",
    when(col("System_Group").isNull(),"1").otherwise("")
)

## Uniformat
uniformat = spark.read.table("IoSF")

## Merging IoSF and df together


df = df.withColumn("System - Uniformat Code", split(col("Uniformat"), "-")[0])
uniformat = spark.read.table("IoSF")


# Perform the join with df on 'Uniformat' and 'System_Uniformat'
df = df.join(
    uniformat,
    df["Uniformat"] == uniformat["System - Uniformat Code"],
    how="left"
)


df = df.withColumn(
    "Mismatch_SystemGroup",
    when(col("System_Group") == "F20 - Selective Building Demolition", "")
    .when(col("System_Group") == "F2020 - Hazardous Components Abatement", "")
    .when(col("System_Group") == "D20 - Plumbing", "")
    .when(col("System_Group") != col("System Group"), "1")
    .otherwise("")
)


## Uniformat error 

df = df.withColumn(
    "Uniformat_Error",
    when(col("Score").isNull(),"1").otherwise("")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_error = df.select("Requirement_EID","Building_ID","Estimated_cost_missing","Impact_missing","LinkedSystem_missing","SystemGroup_missing","Mismatch_SystemGroup","Uniformat_Error")


from pyspark.sql.functions import col

df_error = df_error.filter(
    (col("Requirement_EID") == 1) |
    (col("Building_ID") == 1) |
    (col("Estimated_cost_missing") == 1) |
    (col("Impact_missing") == 1) |
    (col("LinkedSystem_missing") == 1) |
    (col("SystemGroup_missing") == 1) |
    (col("Mismatch_SystemGroup") == 1) |
    (col("Uniformat_Error") == 1)
)
display(df_error)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.functions import count


(
    df_error.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")   # ensure schema is replaced if table exists/was cached
      .saveAsTable("DataValidation")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
