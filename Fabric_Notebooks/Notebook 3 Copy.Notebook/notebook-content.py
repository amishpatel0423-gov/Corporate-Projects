# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bbb3f78c-986d-4f8f-bced-ea71153a6848",
# META       "default_lakehouse_name": "test_lakehouse",
# META       "default_lakehouse_workspace_id": "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e",
# META       "known_lakehouses": [
# META         {
# META           "id": "bbb3f78c-986d-4f8f-bced-ea71153a6848"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests

BASE = "https://goa-prism-blims-prod-backend-app.azurewebsites.net"
LOGIN_URL = f"{BASE}/api/auth/login"         # <-- confirm in DevTools
REPORT_URL = f"{BASE}/api/reports/contact-asset"

# Replace field names to match what you see (could be 'username' vs 'email', etc.)
payload = {
    "email": "crystal.maceachern@gov.ab.ca",
    "password": "mypassword"  # Store securely later (Key Vault/fabric credential)
}

# Some backends expect form-encoded; most modern ones accept JSON:
resp_login = requests.post(LOGIN_URL, json=payload, timeout=60)
# If that fails, try:
# resp_login = requests.post(LOGIN_URL, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded"}, timeout=60)

resp_login.raise_for_status()
login_json = resp_login.json()

# Adjust keys based on actual response
token = (login_json.get("access_token")
         or login_json.get("token")
         or login_json.get("jwt")
         or (login_json.get("data", {}) if isinstance(login_json.get("data"), dict) else {}).get("token"))

assert token, f"Token not found in login response: keys={list(login_json.keys())} body={login_json}"

headers = {"Authorization": f"Bearer {token}"}
resp = requests.get(REPORT_URL, headers=headers, params={"p_contact_id": "PM"}, timeout=120)
print("Status:", resp.status_code, resp.headers.get("Content-Type"))
print(resp.text[:500])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import pandas as pd
from io import StringIO

# --- Endpoint & params ---
url = "https://goa-prism-blims-prod-backend-app.azurewebsites.net/api/reports/contact-asset"
params = {"p_contact_id": "PM"}  # change to the Contact Code you need

# --- TEMPORARY token: copy from browser DevTools -> Network -> Headers ---
headers = {
    "Authorization": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiJ1c2VyX2NyeXN0YWxtYWNlYWNoZXJuIiwiZW1haWwiOiJjcnlzdGFsLm1hY2VhY2hlcm5AZ292LmFiLmNhIiwicm9sZSI6IlVTRVIiLCJzdGF0dXMiOiJBUFBST1ZFRCIsInBlcm1pc3Npb25zIjp7IlByb3BlcnRpZXMgU2VhcmNoIjpbIlJlYWQiXSwiUHJvcGVydGllcyBQYXJjZWxzIjpbIlJlYWQiXSwiUHJvcGVydGllcyBUaXRsZXMiOlsiUmVhZCJdLCJTdHJ1Y3R1cmVzIFNlYXJjaCI6WyJSZWFkIl0sIlN0cnVjdHVyZXMgRmxvb3JzIjpbIlJlYWQiXSwiU3RydWN0dXJlcyBPY2N1cGFudHMiOlsiUmVhZCJdLCJTdHJ1Y3R1cmVzIExlYXNlcyI6WyJSZWFkIl0sIlN0cnVjdHVyZXMgU2VjdGlvbnMiOlsiUmVhZCJdLCJTdHJ1Y3R1cmVzIEZhY2lsaXRpZXMiOlsiUmVhZCJdLCJQYXJraW5nIFNlYXJjaCI6W10sIlNjaG9vbCBGYWNpbGl0aWVzIjpbXSwiSGVhbHRoIEZhY2lsaXRpZXMiOltdLCJSZXBvcnRzIEFjY29tbW9kYXRpb24gQ29zdGluZyI6WyJSZWFkIl0sIlJlcG9ydHMgQWNjb21tb2RhdGlvbiBQbGFubmluZyI6WyJSZWFkIl0sIlJlcG9ydHMgQWN0aXZpdHkiOlsiUmVhZCJdLCJSZXBvcnRzIEVtZXJnZW5jeSBSZXNwb25zaWJsZSBPY2N1cGFudCI6WyJSZWFkIl0sIlJlcG9ydHMgRXZhbHVhdGlvbiI6WyJSZWFkIl0sIlJlcG9ydHMgSFJNUyBMb2MgQ29kZSI6WyJSZWFkIl0sIlJlcG9ydHMgSW52ZW50b3J5IjpbIlJlYWQiXSwiUmVwb3J0cyBMYW5kIjpbIlJlYWQiXSwiUmVwb3J0cyBMZWFzaW5nIjpbIlJlYWQiXSwiUmVwb3J0cyBQYXJraW5nIjpbXSwiUmVwb3J0cyBQcm9qZWN0cyI6WyJSZWFkIl0sIlJlcG9ydHMgU3VwcG9ydGVkIFN0YWtlaG9sZGVycyI6W119LCJpYXQiOjE3NzA0MDk0MDQsImV4cCI6MTc3MDQ5NTgwNCwiYXVkIjoidGVhbS1tYW5hZ2VtZW50LWNsaWVudCIsImlzcyI6InRlYW0tbWFuYWdlbWVudC1hcGkifQ.Xsb1JPqk8TDZiFDFKsZ0cMXK6UYqmBo5A6My2Dfhj44"
}

# --- Call the API ---
resp = requests.get(url, headers=headers, params=params, timeout=60)
resp.raise_for_status()

# Try parse as CSV first; fallback to JSON
text = resp.text
df = None
try:
    df = pd.read_csv(StringIO(text))
except Exception:
    try:
        data = resp.json()
        # If the JSON is a list of rows:
        if isinstance(data, list):
            df = pd.DataFrame(data)
        # If the JSON is an object with 'data' key:
        elif isinstance(data, dict) and "data" in data:
            df = pd.DataFrame(data["data"])
        else:
            # Last resort: make a 1-row frame
            df = pd.DataFrame([data])
    except Exception as e:
        raise RuntimeError(f"Could not parse response as CSV or JSON. First 500 chars:\n{text[:500]}") from e

# --- Save to Lakehouse: both Files (parquet) and Tables (Delta) ---
files_path = "/lakehouse/default/Files/ingest/contact_asset_PM.parquet"
df.to_parquet(files_path, index=False)

# Save as Delta table for easy SQL/Power BI
from datetime import datetime
table_name = "contact_asset_PM"
df["_ingested_utc"] = datetime.utcnow()  # small lineage stamp

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(df)
spark_df.write.mode("overwrite").format("delta").saveAsTable(table_name)

print(f"Saved to {files_path} and Delta table `{table_name}`.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import synapse.ml.services as services
import synapse.ml.services.openai as openai

print("--- Classes in synapse.ml.services ---")
print([item for item in dir(services) if "OpenAI" in item or "Chat" in item])

print("\n--- Classes in synapse.ml.services.openai ---")
print([item for item in dir(openai) if "OpenAI" in item or "Chat" in item])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from synapse.ml.services.openai import OpenAIChatCompletion, OpenAIEmbedding

# This will now work without an ImportError!
print("Success! Classes found.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from synapse.ml.services.openai import OpenAIChatCompletion

chat_model = (OpenAIChatCompletion()
    .setSubscriptionKey("YOUR_AZURE_OPENAI_KEY")
    .setDeploymentName("YOUR_DEPLOYMENT_NAME") # e.g., "gpt-4"
    .setCustomServiceName("YOUR_RESOURCE_NAME")
    .setMessagesCol("messages")     # The column containing your chat history/prompt
    .setErrorCol("error")           # Column for error messages
    .setOutputCol("response"))      # Column for the AI's answer

print("Chat model client is ready!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import os
import fitz # PyMuPDF
from synapse.ml.services.openai import OpenAIChatCompletion
from pyspark.sql.functions import col, lit, array, struct, regexp_replace, from_json, explode, substring

# --- 1. SETUP & PDF EXTRACTION ---
folder_path = "/lakehouse/default/Files/TEST_PDFS/"
table_name = "fact_general_ledger_extracted"
all_data = []

for file_name in [f for f in os.listdir(folder_path) if f.endswith('.pdf')]:
    full_path = os.path.join(folder_path, file_name)
    doc = fitz.open(full_path)
    # Extract text and keep a sample if the file is massive
    text = "".join([page.get_text() for page in doc])
    all_data.append((file_name, text))

if not all_data:
    print("No PDFs found.")
else:
    # Create DF and TRUNCATE the text to ensure GPT-4o has 'buffer' space to respond
    df_raw = spark.createDataFrame(all_data, ["file_name", "raw_text"])
    df_truncated = df_raw.withColumn("safe_text", col("raw_text").substr(0, 20000))

    # --- 2. FORMAT MESSAGES ---
    df_input = df_truncated.withColumn("messages", array(
        struct(lit("system").alias("role"), lit("Extract General Ledger entries. Return ONLY a JSON array of objects. Keys: EntryDate, Description, Debit, Credit, AccountCode. Do not include markdown tags.").alias("content")),
        struct(lit("user").alias("role"), col("safe_text").alias("content"))
    ))

    # --- 3. AI CONFIGURATION ---
    extractor = (OpenAIChatCompletion()
        .setSubscriptionKey("runtime_auth")
        .setEndpoint("runtime_auth")
        .setDeploymentName("gpt-4o")
        .setMessagesCol("messages")
        .setOutputCol("response")
        .setErrorCol("error")
        .setTemperature(0.0)) # Set to 0 for strict data extraction

    df_ai = extractor.transform(df_input)

    # --- 4. PARSING & FLATTENING ---
    gl_schema = ArrayType(StructType([
        StructField("EntryDate", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Debit", StringType(), True),
        StructField("Credit", StringType(), True),
        StructField("AccountCode", StringType(), True)
    ]))

    # Using getItem(0) explicitly to reach the message content
    df_parsed = df_ai.withColumn(
        "raw_ai_text", 
        col("response.choices").getItem(0).getField("message").getField("content")
    ).withColumn(
        "cleaned_json", 
        regexp_replace(col("raw_ai_text"), r"```json|```", "")
    ).withColumn(
        "parsed_data", 
        from_json(col("cleaned_json"), gl_schema)
    )

    # Use 'explode_outer' so we can at least see the file name even if parsing fails
    from pyspark.sql.functions import explode_outer
    df_final = df_parsed.select(
        col("file_name"),
        col("raw_ai_text").alias("DEBUG_AI_RESPONSE"),
        explode_outer(col("parsed_data")).alias("entry")
    ).select(
        "file_name",
        "DEBUG_AI_RESPONSE",
        "entry.*"
    )

    # --- 5. RESULT CHECK ---
    if df_final.filter(col("EntryDate").isNotNull()).count() > 0:
        df_final.drop("DEBUG_AI_RESPONSE").write.mode("overwrite").format("delta").saveAsTable(table_name)
        print(f"SUCCESS: Data written to {table_name}")
        display(df_final.drop("DEBUG_AI_RESPONSE"))
    else:
        print("ALERT: AI returned text but parsing failed. See raw response below:")
        display(df_final.select("file_name", "DEBUG_AI_RESPONSE"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Truncate AGGRESSIVELY to test if it's a length issue
# We take only the first 3000 characters (very safe)
df_small_test = df_raw.limit(1).withColumn("test_text", col("raw_text").substr(0, 3000))

df_input_test = df_small_test.withColumn("messages", array(
    struct(lit("system").alias("role"), lit("Extract 2 rows only from GL as JSON.").alias("content")),
    struct(lit("user").alias("role"), col("test_text").alias("content"))
))

# 2. Run with Metadata enabled
debug_res = (extractor
    .setMaxTokens(500) # Force a small, fast output
    .transform(df_input_test))

# 3. Look at the HIDDEN metadata fields
display(debug_res.select(
    "file_name",
    col("response.choices").getItem(0).getField("finish_reason").alias("REASON"),
    col("response.choices").getItem(0).getField("message").getField("content").alias("CONTENT"),
    "error"
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install -q --no-warn-conflicts google-genai pymupdf

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import fitz # PyMuPDF
import json
from google import genai
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

# --- 1. CONFIGURATION ---
GEMINI_API_KEY = "AIzaSyDiZNEql8OEE_4E8802INRJwbXxIy7ZbIY" # Replace with your valid key
folder_path = "/lakehouse/default/Files/TEST_PDFS/"
table_name = "fact_general_ledger_extracted"

# Initialize client with current 2026 stable API version
client = genai.Client(api_key=GEMINI_API_KEY)

# --- 2. EXTRACT AND PROCESS ---
all_rows = []
for file_name in [f for f in os.listdir(folder_path) if f.endswith('.pdf')]:
    full_path = os.path.join(folder_path, file_name)
    doc = fitz.open(full_path)
    text = "".join([page.get_text() for page in doc])
    
    # --- 3. CALL GEMINI (Using ACTIVE 2026 Model) ---
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash', # UPDATED: Replaces retired 1.5-pro
            contents=f"""
            Extract General Ledger entries. Return ONLY a raw JSON array. 
            Keys: EntryDate, Description, Debit, Credit, AccountCode.
            Text: {text[:40000]}
            """
        )
        
        # Clean Markdown and Parse
        raw_json = response.text.replace("```json", "").replace("```", "").strip()
        data = json.loads(raw_json)
        
        for entry in data:
            all_rows.append({
                "file_name": file_name,
                "EntryDate": str(entry.get("EntryDate", "")),
                "Description": str(entry.get("Description", "")),
                "Debit": str(entry.get("Debit", "")),
                "Credit": str(entry.get("Credit", "")),
                "AccountCode": str(entry.get("AccountCode", ""))
            })
            print(f"Successfully processed {file_name}")
            
    except Exception as e:
        print(f"Error on {file_name}: {e}")

# --- 4. SAVE TO LAKEHOUSE ---
if all_rows:
    df_final = spark.createDataFrame(all_rows)
    df_final.write.mode("overwrite").format("delta").saveAsTable(table_name)
    print(f"SUCCESS: {len(all_rows)} rows written to table.")
    display(df_final)
else:
    print("No data extracted. Verify your API key and file content.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import fitz # PyMuPDF
import json
from google import genai
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

# --- 1. CONFIGURATION ---
GEMINI_API_KEY = "AIzaSyDiZNEql8OEE_4E8802INRJwbXxIy7ZbIY" 
folder_path = "/lakehouse/default/Files/TEST_PDFS/"
table_name = "fact_general_ledger_extracted"

# Initialize the NEW 2026 client structure
client = genai.Client(api_key=GEMINI_API_KEY)

# --- 2. EXTRACT AND PROCESS ---
all_rows = []
for file_name in [f for f in os.listdir(folder_path) if f.endswith('.pdf')]:
    full_path = os.path.join(folder_path, file_name)
    doc = fitz.open(full_path)
    text = "".join([page.get_text() for page in doc])
    
    # --- 3. CALL GEMINI (New API Syntax) ---
    try:
        response = client.models.generate_content(
            model='gemini-1.5-pro',
            contents=f"""
            Extract GL entries. Return ONLY a raw JSON array. 
            Keys: EntryDate, Description, Debit, Credit, AccountCode.
            Text: {text[:40000]}
            """
        )
        
        # Clean Markdown and Parse
        raw_json = response.text.replace("```json", "").replace("```", "").strip()
        data = json.loads(raw_json)
        
        for entry in data:
            all_rows.append({
                "file_name": file_name,
                "EntryDate": str(entry.get("EntryDate", "")),
                "Description": str(entry.get("Description", "")),
                "Debit": str(entry.get("Debit", "")),
                "Credit": str(entry.get("Credit", "")),
                "AccountCode": str(entry.get("AccountCode", ""))
            })
            
    except Exception as e:
        print(f"Error on {file_name}: {e}")

# --- 4. SAVE ---
if all_rows:
    df_final = spark.createDataFrame(all_rows)
    df_final.write.mode("overwrite").format("delta").saveAsTable(table_name)
    print(f"SUCCESS: {len(all_rows)} rows written to table.")
    display(df_final)
else:
    print("No data extracted. Please check your key or file content.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import fitz # PyMuPDF
import json
from google import genai
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

# --- 1. CONFIGURATION ---
GEMINI_API_KEY = "AIzaSyDiZNEql8OEE_4E8802INRJwbXxIy7ZbIY" # Double-check for extra spaces!
folder_path = "/lakehouse/default/Files/TEST_PDFS/"
table_name = "fact_general_ledger_extracted"

# Initialize the NEW client
client = genai.Client(api_key=GEMINI_API_KEY)

# --- 2. EXTRACT AND PROCESS ---
all_rows = []
for file_name in [f for f in os.listdir(folder_path) if f.endswith('.pdf')]:
    full_path = os.path.join(folder_path, file_name)
    doc = fitz.open(full_path)
    text = "".join([page.get_text() for page in doc])
    
    # --- 3. CALL GEMINI ---
    try:
        response = client.models.generate_content(
            model='gemini-1.5-pro',
            contents=f"""
            Extract General Ledger entries from this text. 
            Return ONLY a raw JSON array of objects with these keys: 
            EntryDate, Description, Debit, Credit, AccountCode.
            Text: {text[:40000]} 
            """
        )
        
        # Clean Markdown if present
        raw_json = response.text.replace("```json", "").replace("```", "").strip()
        data = json.loads(raw_json)
        
        for entry in data:
            all_rows.append({
                "file_name": file_name,
                "EntryDate": str(entry.get("EntryDate", "")),
                "Description": str(entry.get("Description", "")),
                "Debit": str(entry.get("Debit", "")),
                "Credit": str(entry.get("Credit", "")),
                "AccountCode": str(entry.get("AccountCode", ""))
            })
            
    except Exception as e:
        print(f"Error processing {file_name}: {e}")

# --- 4. SAVE TO LAKEHOUSE ---
if all_rows:
    df_final = spark.createDataFrame(all_rows)
    df_final.write.mode("overwrite").format("delta").saveAsTable(table_name)
    print(f"SUCCESS: {len(all_rows)} rows written to {table_name}")
    display(df_final)
else:
    print("No data extracted. Verify your API key and file contents.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
