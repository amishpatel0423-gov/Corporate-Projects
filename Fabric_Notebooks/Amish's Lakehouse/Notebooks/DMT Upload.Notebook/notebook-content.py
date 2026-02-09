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

from pyspark.sql.functions import col, lit, regexp_replace, trim,when

vfa = spark.table("VFAExtract")

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
       .withColumnRenamed("Requirement - ID", "Requirement_ID") \
       .withColumnRenamed("Asset - City", "City") \
       .withColumnRenamed("Requirement - System Group", "System_Group") \
       .withColumnRenamed("Requirement - RAP Project Status", "RAPProjectStatus") \
       .withColumnRenamed("Requirement - Ranking #2", "RankingTwo") \
       .withColumnRenamed("Requirement - Renewal", "IsRenewal") \
       .withColumnRenamed("Requirement - Action FY", "Action_FY")

vfa = vfa.withColumn("Uniformat", trim(regexp_replace(col("Linked_System"), "-.*", "")))

vfa = vfa.filter(
    (col("Priority") != "Lifecycle Planning (at least 4 years remaining at inspection)")) \
         .filter(~col("RAPProjectStatus").isin("APP", "UWY", "COM"))

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

# App data
app = spark.table("appData").select("pmbcp_rapid", "pmbcp_statusname")
app = app.dropna()
app = app.withColumnRenamed("pmbcp_rapid","RAPProjectID").withColumnRenamed("pmbcp_statusname","AppStatus")



# Budget 2026 data
b2026 = spark.table("B2026").select("REQ ID")

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

vfa_renewal = vfa_scored.filter(col("Category") == "Lifecycle")
vfa_renewal = vfa_renewal.select("Requirement_ID","Score")

vfa_nonrenewal = vfa_scored.filter(col("Category") != "Lifecycle")
vfa_nonrenewal = vfa_nonrenewal.select("Requirement_ID","Score")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import pandas as pd

excel_path = "/lakehouse/default/Files/DMT_Score_upload.xlsx"

pdf1 = vfa_renewal.toPandas()
pdf2 = vfa_nonrenewal.toPandas()

with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
    pdf1.to_excel(writer, sheet_name="Renewal", index=False)
    pdf2.to_excel(writer, sheet_name="Non Renewal", index=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# PySpark / Python
import os
import json
import requests
from datetime import datetime
import zoneinfo

## adding time stamp to the file name 

tz = zoneinfo.ZoneInfo("America/Edmonton")
now_local = datetime.now(tz)
date_stamp = now_local.strftime("%Y%m%d")

base_name = "DMT_Score_upload"
file_name = f"{base_name}"

# =========================
# Configuration Parameters
# =========================
site          = "S300D13-PMB-CORP-PROJ13424"             # SharePoint site name
drive         = "CMR"                        # SharePoint document library
client_id     = "pmpi-app-prod"   # Azure App Registration client ID
client_secret = "4k9lzh5Fa35rPsxmIk0q2Zc0TsJEBtgK"  # Azure App Registration client secret
file_path     = r"/lakehouse/default/Files/DMT_Score_upload.xlsx"  
target_path   = f"/DMTScoreUploads/{file_name}.xlsx"              # Destination in SharePoint library
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
