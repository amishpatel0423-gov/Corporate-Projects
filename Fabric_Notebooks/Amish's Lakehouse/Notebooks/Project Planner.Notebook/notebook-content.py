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

from pyspark.sql.functions import regexp_replace, trim,when,col,countDistinct, max as _max 
from pyspark.sql import functions as F
## Setting up the VFA data 

vfa = spark.table("VFAExtract")

## adding a comment

vfa = vfa.withColumnRenamed("Asset - Portfolio Name", "Portfolio_Name") \
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

vfa = vfa.withColumn("Uniformat", trim(regexp_replace(col("Linked_System"), "-.*", "")))
cvfa = vfa.count()
display(cvfa)

vfa = vfa.filter(
    (F.col("RAPProjectStatus").isNull()) |
    (F.col("RAPProjectStatus") == "") |
    (~F.col("RAPProjectStatus").isin("APP", "UWY", "COM", "COP"))
)



#vfa = vfa.filter(
    #(col("System_Group").isNotNull()) &
    #(col("Property_Name") != "Lethbridge Recovery Community") &
    #(col("Property_Name") != "Red Deer Recovery Community")
#)


cvfa = vfa.count()
display(cvfa)


vfa = vfa.withColumn(
    "Includes_Renewal",
    when((col("Category") == "Lifecycle") & (col("IsRenewal") == "TRUE"), "Yes").otherwise("No")
)



group_flags = (
    vfa
    .groupBy("Building_ID", "System_Group")
    .agg(
        # 1 if any row in the group has Category == 'Study'
        _max(when(col("Category") == "_Study [Non-FCI]", 1).otherwise(0)).alias("HasStudy"),
        # 1 if any row in the group has a non-null Category != 'Study'
        _max(when((col("Category").isNotNull()) & (col("Category") != "Study"), 1).otherwise(0)).alias("HasNonStudy")
    )
)

# 2) Join flags back to the main DF
vfa = vfa.join(group_flags, on=["Building_ID", "System_Group"], how="left")

# 3) Create both columns in one chained call
vfa = (vfa.withColumn(
        "CI_Only",
        when((col("HasStudy") == 1) & (col("HasNonStudy") == 0), "Yes").otherwise("No")
    )
    .withColumn(
        "Includes_CI",
        when((col("HasStudy") == 1) & (col("HasNonStudy") == 1), "Yes").otherwise("No")
    )
    .drop("HasStudy", "HasNonStudy")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import regexp_replace, trim,when,col


# IoAF
ioaf = spark.table("IoAF").select("Asset Use", "Score with Weight")
ioaf = ioaf.withColumnRenamed("Asset Use", "Use").withColumnRenamed("Score with Weight", "ioafScore")

# PlanforAsset
pfa = spark.table("PlanforAsset").select("Building Priority Detail", "Score with Weight")
pfa = pfa.withColumnRenamed("Score with Weight", "pfaScore")

# IoSF
iosf = spark.table("IoSF").select("System - Uniformat Code", "Score with Weight")
iosf = iosf.withColumnRenamed("System - Uniformat Code", "UniformatCode").withColumnRenamed("Score with Weight", "iosfScore")

# Requirement Priority
reqpriority = spark.table("`Requirement Priority`").select("Requirement Priority", "Score with Weight")
reqpriority = reqpriority.withColumnRenamed("Score with Weight", "reqpriorityScore")

# Requirement Category
reqcategory = spark.table("`Requirement_Category`").select("Requirement Category", "Score with Weight")
reqcategory = reqcategory.withColumnRenamed("Score with Weight", "reqcategoryScore")

# Requirement Impact
reqimpact = spark.table("`Requirement Impact`").select("Requirement Impact", "Score with Weight")
reqimpact = reqimpact.withColumnRenamed("Score with Weight", "reqimpactScore")

# Building List
buildingList = spark.table("cr914_building_list")
buildingList = buildingList.select(
    col("cr914_bldgid").alias("Building_ID"),
    col("cr914_location").alias("Location"),
    col("cr914_directorlevelcontactname").alias("Director"),
    col("cr914_facilitymanagercontactname").alias("FM"),
    col("cr914_facilitycoordinatorcontactname").alias("FC"),
    col("cr914_tpmcontactname").alias("TPM")
)

buildingList = buildingList.withColumn("TPM", when((col("TPM") == "") | col("TPM").isNull(), "Inhouse").otherwise(col("TPM")))

# App data
app = spark.table("appData").select("pmbcp_rapid", "pmbcp_statusname")
app = app.dropna()
app = app.withColumnRenamed("pmbcp_rapid","RAPProjectID").withColumnRenamed("pmbcp_statusname","AppStatus")

cutoffscore = 85


# Budget 2026 data
b2026 = spark.table("B2026").select("REQ ID")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, expr
from pyspark.sql.functions import when, col,ceil
from pyspark.sql.functions import col, max as spark_max, sum as spark_sum, expr
from pyspark.sql.functions import concat_ws, ceil, sum as spark_sum,sort_array,collect_set
from pyspark.sql import functions as F



# Perform left joins
vfa_scored = vfa.join(ioaf, vfa["Use_Type"] == ioaf["Use"], "left") \
    .join(pfa, vfa["Building_Priority_Detail"] == pfa["Building Priority Detail"], "left") \
    .join(reqcategory, vfa["Category"] == reqcategory["Requirement Category"], "left") \
    .join(reqpriority, vfa["Priority"] == reqpriority["`Requirement Priority`"], "left") \
    .join(reqimpact, vfa["Impact"] == reqimpact["`Requirement Impact`"], "left")\
    .join(iosf, vfa["Uniformat"] == iosf["UniformatCode"], "left")

    
# Assuming vfa_scored is a PySpark DataFrame
vfa_scored = vfa_scored.fillna({
    'iosfScore': 0,
    'ioafScore': 0,
    'pfaScore': 0,
    'reqcategoryScore': 0,
    'reqpriorityScore': 0,
    'reqimpactScore': 0
})



# Calculate the total score
vfa_scored = vfa_scored.withColumn(
    "Score",
    col("ioafScore") + col("pfaScore") + col("reqcategoryScore") + col("reqpriorityScore") + col("reqimpactScore") + col("iosfScore")
)


columns_to_drop = ["ioafScore", "pfaScore", "reqcategoryScore", "reqpriorityScore", "reqimpactScore", "UniformatCode"]
vfa = vfa_scored.drop(*columns_to_drop)

# PLanned or Unplanned
planned_ids = [row[0] for row in b2026.select(col("`REQ ID`").cast("string")).distinct().collect()]

vfa = vfa.withColumn("Requirement_EID", col("Requirement_EID").cast("string"))

vfa = vfa.withColumn(
    "planned",
    when(col("Requirement_EID").isin(planned_ids), "Planned").otherwise("Unplanned")
)


vfa = vfa.withColumn(
    "CostFactoredTPC",
    ceil(when(col("Category") == "_Study [Non-FCI]", col("EstimatedCost"))
    .otherwise(col("EstimatedCost") * 1.48))
)


# Assuming vfa and appdata are PySpark DataFrames
vfa = vfa.join(app,"RAPProjectID","left")

display(vfa)

RequirementList = vfa.select("Portfolio_Name", "Property_Name", "Building_ID","Building_Priority_Detail","Use","System_Group","Linked_System","Uniformat",
"Score","Requirement_EID","Requirement_Name","Category","IsRenewal","RAPProjectID","RAPProjectStatus","AppStatus","planned",
"EstimatedCost","CostFactoredTPC","Includes_CI","CI_Only","Includes_Renewal","Action_FY")
RequirementList = RequirementList.withColumn("Cuttoffscore", when(col("Score") >= cutoffscore, "Over").otherwise("Under"))
RequirementList = RequirementList.join(buildingList, on="Building_ID", how="inner")


ProjectReview = RequirementList.select("Building_ID", "Property_Name", "System_Group","Score","Requirement_EID","Category",
"Linked_System","Requirement_Name","EstimatedCost","CostFactoredTPC","RAPProjectID","RAPProjectStatus","AppStatus","planned",
"Cuttoffscore","Includes_CI","CI_Only","Includes_Renewal","Action_FY")

#display(RequirementList)
rlc = RequirementList.count()
display(rlc)

from pyspark.sql import functions as F

ProjectReview = ProjectReview.groupBy(
    "Building_ID", "Property_Name", "System_Group", "Linked_System"
).agg(
    F.max("Score").alias("Score"),
    F.expr("max_by(Cuttoffscore, Score)").alias("Cuttoffscore"),  # <-- This matches max Score
    F.expr("max_by(Requirement_EID, Score)").alias("ReqID"),
    F.expr("max_by(Requirement_Name, Score)").alias("ReqName"),
    F.sum("EstimatedCost").alias("EstCost"),
    F.sum("CostFactoredTPC").alias("CostFactoredTPC"),
    F.when(
        F.max(F.when(F.col("planned") == "Planned", 1).otherwise(0)) == 1, "Planned"
    ).otherwise("Unplanned").alias("Planned_vs_Unplanned"),
    F.expr("max_by(Action_FY, Score)").alias("Action_FY"),
    F.expr("max_by(RAPProjectID, Score)").alias("RAPProjectID"),
    F.expr("max_by(RAPProjectStatus, Score)").alias("RAPProjectStatus"),
    F.expr("max_by(AppStatus, Score)").alias("AppStatus"),
    F.sum(F.when(F.col("Includes_Renewal") == "Yes", F.col("CostFactoredTPC")).otherwise(0)).alias("Renewal_Cost"),
    F.sum(F.when(F.col("Category") != "Lifecycle", F.col("CostFactoredTPC")).otherwise(0)).alias("Non_Renewal_Cost")
)

# Step 1: Add ProjectName column
ProjectReview = ProjectReview.withColumn(
    "ProjectName",
    concat_ws(" - ", col("Building_ID"), col("System_Group"))
)

ProjectReview = ProjectReview.withColumn(
    "Renewal_Percentage",
    col("Renewal_Cost") / col("CostFactoredTPC") * 100
)

ProjectReview = ProjectReview.withColumn(
    "Non_Renewal_Percentage",
    col("Non_Renewal_Cost") / col("CostFactoredTPC") * 100
)


# Step 2: Select relevant columns
ProjectReview = ProjectReview.select(
    "Building_ID", "Property_Name","ProjectName", "System_Group","Score","ReqID","Linked_System","ReqName","EstCost", 
    "CostFactoredTPC","RAPProjectID","AppStatus","Planned_vs_Unplanned","Action_FY","Cuttoffscore","Renewal_Cost","Non_Renewal_Cost","Renewal_Percentage","Non_Renewal_Percentage"
)



#display(ProjectReview)
prc = ProjectReview.count()
display(prc)
display(ProjectReview)

ProjectReview = ProjectReview.join(buildingList, on="Building_ID", how="inner")


# Step 3: Group and summarize with ceiling
ProjectSummary = ProjectReview.groupBy("Building_ID","System_Group", "Property_Name").agg(
    F.max("Score").alias("Score"),
    F.expr("max_by(ProjectName, Score)").alias("ProjectName"),
    F.expr("max_by(RAPProjectID, Score)").alias("RAPProjectID"),
    F.expr("max_by(AppStatus, Score)").alias("AppStatus"),
    F.expr("max_by(Cuttoffscore, Score)").alias("Cuttoffscore"),
    F.expr("max_by(Action_FY, Score)").alias("Action_FY"),
    ceil(spark_sum("EstCost")).alias("EstCost"),
    ceil(spark_sum("CostFactoredTPC")).alias("CostFactoredTPC"),
    F.when(
        F.max(F.when(F.col("Planned_vs_Unplanned")=="Planned",1).otherwise(0))==1,
        "Planned"
        ).otherwise("Unplanned").alias("Planned_vs_Unplanned"),
    ceil(spark_sum("Renewal_Cost")).alias("Renewal_Cost"),
    ceil(spark_sum("Non_Renewal_Cost")).alias("Non_Renewal_Cost")
)

ProjectSummary = ProjectSummary.withColumn(
    "Renewal_Percentage",
    col("Renewal_Cost") / col("CostFactoredTPC") * 100
)

ProjectSummary = ProjectSummary.withColumn(
    "Non_Renewal_Percentage",
    col("Non_Renewal_Cost") / col("CostFactoredTPC") * 100
)



ProjectSummary = ProjectSummary.withColumn(
    "Proposed_Funding_Source",
    when(col("Renewal_Percentage") == 100, "Capital To Be Confirmed")
    .when((col("Renewal_Percentage") == 0) & (col("Non_Renewal_Percentage") == 100), "Operating To Be Confirmed")
    .when((col("Renewal_Percentage").isNotNull()) & (col("Renewal_Percentage") != 0) & (col("Renewal_Percentage") >= 51), "Capital To Be Confirmed")
    .when((col("Renewal_Percentage") == 50) & (col("Non_Renewal_Percentage") == 50), "Capital To Be Confirmed")
    .when((col("Non_Renewal_Percentage").isNotNull()) & (col("Non_Renewal_Percentage") >= 51), "Operating To Be Confirmed")
    .otherwise("Capital To Be Confirmed")
)


# Step 2: Join with project_summary on "Number"
ProjectSummary = ProjectSummary.join(buildingList, on="Building_ID", how="inner")





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, ceil, lit, greatest,desc
# Initialize Spark session
spark = SparkSession.builder.appName("ProjectCashflow").getOrCreate()

# Constants
CashFlowSplitY1 = 0.4
CashFlowSplitY2 = 0.6
CashFlowY1 = 0.25
CashFlowY2 = 0.4
CashFlowY3 = 0.35
CostFactorPercentage = 1.48
MaxEstCostWarning = 5000000

# Assuming df is your project_summary DataFrame
df = ProjectSummary

# Year1 calculation
df = df.withColumn("Year1", when(col("CostFactoredTPC") < 500000, ceil(col("CostFactoredTPC")))
    .when((col("CostFactoredTPC") > 500000) & (col("CostFactoredTPC") < 2000000), col("CostFactoredTPC") * CashFlowSplitY1)
    .otherwise(col("CostFactoredTPC") * CashFlowY1))

# Year2 calculation
df = df.withColumn("Year2", when(col("Year1") == col("CostFactoredTPC"), lit(0))
    .when((col("CostFactoredTPC") > 500000) & (col("CostFactoredTPC") < 2000000), col("CostFactoredTPC") * CashFlowSplitY2)
    .when(col("CostFactoredTPC") > MaxEstCostWarning, lit(MaxEstCostWarning))
    .otherwise(col("CostFactoredTPC") * CashFlowY2))

# Year3 calculation
df = df.withColumn("Year3", when((col("Year1") + col("Year2")) == col("CostFactoredTPC"), lit(0))
    .when((col("CostFactoredTPC") * CashFlowY3) > MaxEstCostWarning, lit(MaxEstCostWarning))
    .otherwise(col("CostFactoredTPC") * CashFlowY3))

# CashFlowFutureYears calculation
df = df.withColumn("CashFlowFutureYears", greatest(
    when((col("Year1") + col("Year2") + col("Year3")) == col("CostFactoredTPC"), lit(0))
    .otherwise(col("CostFactoredTPC") - (col("Year1") + col("Year2") + col("Year3"))),
    lit(0)
))


df = df.withColumn("Year1", ceil(col("Year1")))
df = df.withColumn("Year2", ceil(col("Year2")))
df = df.withColumn("Year3", ceil(col("Year3")))
df = df.orderBy(desc("Score"))

(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")   # ensure schema is replaced if table exists/was cached
      .saveAsTable("ProjectCashFlow")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql.functions import col


(
    RequirementList.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")   # ensure schema is replaced if table exists/was cached
      .saveAsTable("RequirementList")
)

(
    ProjectReview.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")   # ensure schema is replaced if table exists/was cached
      .saveAsTable("ProjectReview")
)

(
    ProjectSummary.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")   # ensure schema is replaced if table exists/was cached
      .saveAsTable("ProjectSummary")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Saving it as a file in lakehouse as previous version of Monthly planner in Excel format to upload it to sharepoint folder

from datetime import datetime
import zoneinfo
import pandas as pd
import os
import json
import requests

excel_path = "/lakehouse/default/Files/MonthlyPlanner.xlsx"

s1 = RequirementList.toPandas()
s2 = ProjectReview.toPandas()
s3 = ProjectSummary.toPandas()

with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
    s1.to_excel(writer, sheet_name="RequirementList", index=False)
    s2.to_excel(writer, sheet_name="ProjectReview", index=False)
    s3.to_excel(writer, sheet_name="ProjectSummary", index=False)

## adding time stamp to the file name 

tz = zoneinfo.ZoneInfo("America/Edmonton")
now_local = datetime.now(tz)
date_stamp = now_local.strftime("%Y%m%d")

base_name = "MonthlyPlanner"
file_name = f"{base_name}_{date_stamp}"



# =========================
# Configuration Parameters
# =========================
site          = "S300D13-PMB-CORP-PROJ13424"             # SharePoint site name
drive         = "CMR"                        # SharePoint document library
client_id     = "pmpi-app-prod"   # Azure App Registration client ID
client_secret = "4k9lzh5Fa35rPsxmIk0q2Zc0TsJEBtgK"  # Azure App Registration client secret
file_path     = r"/lakehouse/default/Files/MonthlyPlanner.xlsx"  
target_path   = f"/MonthlyPlannerUploads/{file_name}.xlsx"              # Destination in SharePoint library
scope         = "SHAREPOINT:USER"

# URLs for UAT environment
token_url     = "https://api.iam.alberta.ca/auth/realms/sa/protocol/openid-connect/token"
api_base_url  = "https://api.alberta.ca/sharepoint"
upload_url    = f"{api_base_url}/v1/createDocument"

# =========================
# 1: Get OAuth Access Token
# =========================
token_payload = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": scope,
}
token_headers = {"Content-Type": "application/x-www-form-urlencoded"}

token_resp = requests.post(token_url, data=token_payload, headers=token_headers, timeout=30)
if not token_resp.ok:
    raise RuntimeError(f"[ERROR] Failed to get access token: {token_resp.status_code} {token_resp.text}")

token_json = token_resp.json()
access_token = token_json.get("access_token")
if not access_token:
    raise RuntimeError("[ERROR] Access token is empty or missing.")

# =========================
# 2: Prepare File and Metadata
# =========================
if not os.path.isfile(file_path):
    raise FileNotFoundError(f"[ERROR] File not found: {file_path}")

file_info = {
    "siteName": site,
    "driveName": drive,
    "contentType": "Document GoA",
    "filePath": target_path,
}

# =========================
# 3: Build Multipart Form Data & Upload
# =========================

# We use 'with open' to safely handle the file stream.
# Note: Unlike R, we don't need to write 'fileInfo' to a temporary disk file;
# we can pass the JSON string directly as an in-memory file.

with open(file_path, "rb") as f:
    files = {
        # This replaces the R 'temp_json_path' logic
        "fileInfo": ("fileInfo.json", json.dumps(file_info), "application/json"),
        
        # This replaces the R 'upload_file(file_path...)'
        "file": (os.path.basename(file_path), f, "application/octet-stream"),
    }
    
    headers = {"Authorization": f"Bearer {access_token}"}

    print(f"Uploading to: {upload_url}")

    try:
        # Perform the request
        # 'requests' automatically handles the multipart boundaries/encoding
        upload_resp = requests.post(upload_url, headers=headers, files=files, timeout=120)

 # =========================
 # 4: Handle Response
# =========================
        if upload_resp.status_code in [200, 201]:
            print("[SUCCESS] File uploaded successfully!")
            print(upload_resp.text)
        else:
            print(f"[WARNING] Upload failed with status: {upload_resp.status_code}")
            print(upload_resp.text)

    except Exception as e:
        print(f"[ERROR] An exception occurred during upload: {e}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
