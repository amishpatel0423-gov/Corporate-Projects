# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "13ba4b10-e8a7-4302-8eea-4f35eb2aead9",
# META       "default_lakehouse_name": "LH_DEV_CP_CMR",
# META       "default_lakehouse_workspace_id": "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e",
# META       "known_lakehouses": [
# META         {
# META           "id": "13ba4b10-e8a7-4302-8eea-4f35eb2aead9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, BooleanType, StringType

# Initialize Spark Session (if not already running, e.g., in a Databricks/Fabric notebook)
spark = SparkSession.builder.appName("VFA_RAP_Staging").getOrCreate()

# --- 1. Load Source DataFrames directly from the Lakehouse ---
# The RAP_SSRS and VFA_SSRS queries in M language pull from the SSRS report URLs,
# but your file indicates they are now loaded as tables in the Lakehouse.

try:
    # 1.1 VFA Requirements Data (Source for VFA_SSRS)
    vfa_ssrs_raw_df = spark.table("VFA_SSRS")

    # 1.2 RAP Project Header Data (Source for RAP_SSRS)
    rap_ssrs_df = spark.table("RAP_SSRS").select("RAPID", "Record_Created")
    
    # 1.3 Building Location Data (Source for cr914_building_list, assumed to be loaded/aliased as such)
    # The original M code joins on 'AssetID' (renamed from cr914_bldgid) and uses 'Location'.
    building_list_df = spark.table("building_list").select(
        F.col("AssetID"), F.col("Location")
    )
except Exception as e:
    print(f"Error loading source tables from Lakehouse: {e}")
    # Handle environment specific table names if needed.

# --- 2. VFA_SSRS Staging Function ---

def vfa_ssrs_staging(vfa_df, rap_df, building_df):
    
    df = vfa_df
    
    # [cite_start]Type Casting and Filtering (M language steps [cite: 186, 190])
    df = df.withColumns({
        "EstimatedCost": F.col("EstimatedCost").cast(LongType()),
        "IsRenewal": F.col("IsRenewal").cast(BooleanType()),
        "OutForAudit": F.col("OutForAudit").cast(BooleanType()),
        "RAPProjectID": F.trim(F.col("RAPProjectID")),
    })
    
    # [cite_start]Filter: Portfolio_Name starts with "PR" [cite: 190]
    df = df.filter(F.col("Portfolio_Name").startswith("PR"))
    
    # [cite_start]Join to RAP_SSRS (Inner Join) [cite: 191]
    # Ensures we only keep requirements linked to active RAP projects.
    df = df.join(rap_df.select("RAPID"), F.col("RAPProjectID") == F.col("RAPID"), "inner")
    
    # [cite_start]Data Cleaning (Replace Win1252 Encoded Dash) [cite: 191]
    df = df.withColumn("Portfolio_Name", F.regexp_replace(F.col("Portfolio_Name"), "â€“", "-"))
    df = df.withColumn("System_Name", F.regexp_replace(F.col("System_Name"), "â??", "-"))

    # [cite_start]Remove Blank Rows (complex PySpark requirement - we only keep non-blank rows) [cite: 191]
    # NOTE: PySpark doesn't have a direct 'List.RemoveMatchingItems' step, this is a simplified version.
    # df = df.filter(F.size(F.array(*[F.when(F.col(c).isNull() | (F.col(c) == ""), F.lit(1)) for c in df.columns])) != F.lit(len(df.columns)))
    
    # [cite_start]Set Blank Use_Type to "Site" [cite: 192]
    df = df.withColumn("Use_Type", F.when(F.col("Use_Type") == "", F.lit("Site")).otherwise(F.col("Use_Type")))

    # [cite_start]Uniformat Fix (Adding spaces for exact match) [cite: 197-200]
    def uniformat_fix(uniformat):
        if uniformat is None: return None
        pos = uniformat.find("-")
        if pos != -1 and pos > 0:
            return uniformat[:pos] + " - " + uniformat[pos + 1:]
        return uniformat

    uniformat_udf = F.udf(uniformat_fix, StringType())
    df = df.withColumn("Uniformat", uniformat_udf(F.col("Uniformat")))

    # [cite_start]Replace all blanks (empty string) to null [cite: 201]
    null_cols = ["RequirementName", "RequirementDescription", "Category", "Priority", "Impact", "Uniformat", "System_Group"]
    df = df.replace("", None, subset=null_cols)
    
    # [cite_start]Replace Null Est Cost with 0 [cite: 202]
    df = df.withColumn("EstimatedCost", F.coalesce(F.col("EstimatedCost"), F.lit(0)))

    # [cite_start]Rename Columns (M language step) [cite: 193]
    rename_map = {
        "Portfolio_Name": "PortfolioName", "Asset_Name": "AssetName", "Building_ID": "AssetID",
        "RAPProjectID": "RAPID", "Requirement_EID": "REQID", "RAPProjectStatus": "RAPStatus",
        "Use_Type": "AssetUse", "Building_Priority_Detail": "BuildingPriorityDetail",
        "System_Group": "VFASystemGroup", "System_Name": "SystemName"
    }
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # [cite_start]Join to Building List [cite: 194]
    df = df.join(building_df, ["AssetID"], "left_outer")

    # [cite_start]Select and Reorder final columns (M language End step) [cite: 202]
    final_cols = [
        "RAPID", "RAPStatus", "PortfolioName", "AssetName", "AssetID", "AssetUse", 
        "BuildingPriorityDetail", "REQID", "RequirementName", "RequirementDescription", 
        "Category", "Priority", "Impact", "EstimatedCost", "SystemName", "Uniformat", 
        "VFASystemGroup", "System_EID", "OutForAudit", "IsRenewal", "Location"
    ]
    
    return df.select(*final_cols)

# --- 3. Execute and Review Staging Data ---

vfa_ssrs_staged_df = vfa_ssrs_staging(vfa_ssrs_raw_df, rap_ssrs_df, building_list_df)

# Show the result of the staging query
vfa_ssrs_staged_df.show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
