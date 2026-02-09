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


from pyspark.sql.functions import regexp_replace, trim, when, col,lit
from functools import reduce
from pyspark.sql import functions as F

from pyspark.sql.window import Window


# 2023
B2023 = spark.table("B2023").select("REQ ID")
B2023 = B2023.withColumnRenamed("REQ ID", "Requirement_EID")
B2023 = B2023.withColumn("FY", lit("2023"))

# 2024
B2024 = spark.table("B2024").select("Requirement - ID")
B2024 = B2024.withColumnRenamed("Requirement - ID", "Requirement_EID")
B2024 = B2024.withColumn("FY", lit("2024"))

# 2025
B2025 = spark.table("B2025").select("REQ ID")
B2025 = B2025.withColumnRenamed("REQ ID", "Requirement_EID")
B2025 = B2025.withColumn("FY", lit("2025"))

# 2026
B2026 = spark.table("B2026").select("REQ ID")
B2026 = B2026.withColumnRenamed("REQ ID", "Requirement_EID")
B2026 = B2026.withColumn("FY", lit("2026"))

tables = [B2023, B2024, B2025, B2026]

all_years = reduce(lambda a, b: a.unionByName(b), tables)



w = Window.partitionBy("Requirement_EID")

dfr = all_years.withColumn("count", F.count(F.lit(1)).over(w))


w_ordered = Window.partitionBy("Requirement_EID").orderBy("FY")

dfr = (dfr
       .withColumn("appearance",
                   F.when(F.row_number().over(w_ordered) == 1, F.lit("New"))
                    .otherwise(F.lit("Repeat"))
                  )
      )


w_fill = (
    Window
    .partitionBy("Requirement_EID")
    .orderBy(F.col("FY").cast("int"))
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

dfr = (dfr
    # set OriginalFYYear only when appearance == "New"
    .withColumn(
        "OriginalFYYear",
        F.when(F.col("appearance") == "New", F.col("FY")).otherwise(F.lit(None))
    )
    # forward fill within each Requirement_EID (equivalent to zoo::na.locf)
    .withColumn(
        "OriginalFYYear",
        F.last("OriginalFYYear", ignorenulls=True).over(w_fill)
    )
)


from pyspark.sql.functions import col


(
    dfr.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")   # ensure schema is replaced if table exists/was cached
      .saveAsTable("RepeatRequirements")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
