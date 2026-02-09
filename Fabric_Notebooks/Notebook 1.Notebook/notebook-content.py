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

# MAGIC %%sparkr
# MAGIC # Welcome to your new notebook
# MAGIC # Type here in the cell editor to add code!
# MAGIC library(httr)
# MAGIC library(jsonlite)
# MAGIC library(readxl)
# MAGIC 
# MAGIC # Configuration
# MAGIC site          <- "S500D27-APIWS01-Test"
# MAGIC drive         <- "Documents"
# MAGIC client_id     <- "sharepoint-client-poc1-app-uat"
# MAGIC client_secret <- "e27079bf-5992-402b-bc30-297c0571e084"
# MAGIC scope         <- "SHAREPOINT:USER"
# MAGIC filePath      <- "/test/VFA.docx"
# MAGIC 
# MAGIC token_url     <- "https://idpdev.gov.ab.ca/auth/realms/ServiceIntegration/protocol/openid-connect/token"
# MAGIC api_base_url  <- "https://apiuat.gov.ab.ca/uat/sharepoint"
# MAGIC download_url  <- paste0(api_base_url, "/v1/getDocument")
# MAGIC 
# MAGIC # Get token
# MAGIC token_response <- POST(
# MAGIC     url = token_url,
# MAGIC     body = list(
# MAGIC         grant_type = "client_credentials",
# MAGIC         client_id = client_id,
# MAGIC         client_secret = client_secret,
# MAGIC         scope = scope
# MAGIC     ),
# MAGIC     encode = "form"
# MAGIC )
# MAGIC 
# MAGIC access_token <- content(token_response)$access_token
# MAGIC 
# MAGIC # Prepare query parameters
# MAGIC query_params <- list(
# MAGIC     siteName = site,
# MAGIC     driveName = drive,
# MAGIC     filePath = filePath
# MAGIC )
# MAGIC 
# MAGIC # Request document via GET
# MAGIC download_response <- httr::GET(
# MAGIC     url = download_url,
# MAGIC     query = query_params,
# MAGIC     add_headers(Authorization = paste("Bearer", access_token))
# MAGIC )
# MAGIC 
# MAGIC # Load Excel into DataFrame
# MAGIC if (status_code(download_response) == 200) {
# MAGIC     raw_data <- content(download_response, "raw")
# MAGIC     temp_file <- tempfile(fileext = ".xlsx")
# MAGIC     writeBin(raw_data, temp_file)
# MAGIC     
# MAGIC     df <- read_excel(temp_file)
# MAGIC     print(df)
# MAGIC } else {
# MAGIC     cat("[WARNING] Download failed with status:", status_code(download_response), "\n")
# MAGIC     print(content(download_response, "text"))
# MAGIC }

# METADATA ********************

# META {
# META   "language": "r",
# META   "language_group": "synapse_pyspark"
# META }
