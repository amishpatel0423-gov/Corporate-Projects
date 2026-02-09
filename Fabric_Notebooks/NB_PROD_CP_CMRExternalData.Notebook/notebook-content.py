# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0a08f46d-4283-4451-a6c8-c231e572f46c",
# META       "default_lakehouse_name": "LH_PROD_CP_CMR",
# META       "default_lakehouse_workspace_id": "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e",
# META       "known_lakehouses": [
# META         {
# META           "id": "0a08f46d-4283-4451-a6c8-c231e572f46c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ProjectSummaryData and VFADataValidation External Data for CMR Request App - VERSION 1.0
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, BooleanType, StringType, DoubleType, StructType, StructField
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("VFA_RAP_Notebook - PRODUCTION").getOrCreate()

# --- 0. Load All Lookup and Source Tables from Lakehouse ---
try:
    configuration_df = spark.table("PROD_ConfigurationTable")
    iosf_df = spark.table("PROD_IoSF")
    ioaf_df = spark.table("PROD_IoAF")
    impactoffailure_df = spark.table("PROD_ImpactOfFailure")
    conditionofsystem_df = spark.table("PROD_ConditionOfSystem")
    planforasset_df = spark.table("PROD_PlanForAsset")
    reasonforwork_df = spark.table("PROD_ReasonForWork")
    vfa_ssrs_raw_df = spark.table("PROD_VFA_SSRS")
    rap_ssrs_df = spark.table("PROD_RAP_SSRS").select(
        "RAPID", "Record_Created", "RAPStatus", 
        "RAPTitle", "RAPProjectDescription", "RAPProjectReason", "RAPProjectConsequence", "RAPProjectComments", 
        "RAPTitleLengthCheck", "RAPDescCheck", "RAPConseqCheck", "RAPReasonCheck", "RAPDataValidation"
    )
    annual_planner_b2025_df = spark.table("PROD_AnnualPlanner_B2025")
    annual_planner_b2026_df = spark.table("PROD_AnnualPlanner_B2026")
except Exception as e:
    print(f"Error loading source tables from Lakehouse: {e}")
    raise

# --- 1. Helper Function and Constants ---
def fn_get_parameter(config_name):
    try:
        row = configuration_df.filter(F.lower(F.col("ConfigurationName")) == F.lower(F.lit(config_name))).head()
        if row:
            config_dict = row.asDict()
            value = config_dict.get("Value")
            message_level = F.lit(config_dict.get("MessageLevel") or 0).cast(LongType()).alias("MessageLevel")
            message_category = F.lit(config_dict.get("MessageCategory") or "").cast(StringType()).alias("MessageCategory")
            return (value, message_level, message_category)
        return (None, F.lit(None).alias("MessageLevel"), F.lit(None).alias("MessageCategory"))
    except Exception:
        return (None, F.lit(None).alias("MessageLevel"), F.lit(None).alias("MessageCategory"))

# Fetch Parameters
PROJECT_WARN_COSTMSG = fn_get_parameter("msg_warnProjSummaryCost")[0]
PROJECT_INFO_COSTMSG = fn_get_parameter("msg_infoProjSummaryCost")[0]
VFADATA_WARN_REQESTCOSTVAL = fn_get_parameter("vfadata_warnReqEstCostVal")[0]
WARN_COST_VAL = float(VFADATA_WARN_REQESTCOSTVAL) if VFADATA_WARN_REQESTCOSTVAL is not None else 2000000.0

# Fetch Messages
MSG_PASS_REQESTCOST = fn_get_parameter("msg_passReqEstCost")[0]
MSG_PASS_REQIMPACT = fn_get_parameter("msg_passReqImpact")[0]
MSG_PASS_REQCATEGORY = fn_get_parameter("msg_passReqCategory")[0]
MSG_PASS_REQDESC = fn_get_parameter("msg_passReqDesc")[0]
MSG_PASS_REQNAME = fn_get_parameter("msg_passReqName")[0]
MSG_PASS_REQPRIORITY = fn_get_parameter("msg_passReqPriority")[0]
MSG_PASS_REQUNIFORMAT = fn_get_parameter("msg_passReqUniformat")[0]
MSG_PASS_REQSYSGRP = fn_get_parameter("msg_passReqSysGrp")[0]
MSG_PASS_RAPDESC = fn_get_parameter("msg_passRAPDescValidation")[0]
MSG_PASS_RAPCONS = fn_get_parameter("msg_passRAPConsValidation")[0]
MSG_PASS_RAPREASON = fn_get_parameter("msg_passRAPReasonValidation")[0]

#--ERROR MESSAGES -- 
MSG_ERROR_REQESTCOST_RENEWAL = fn_get_parameter("msg_errorReqEstCostRenewal")[0]
MSG_ERROR_REQESTCOST_NONRENEWAL = fn_get_parameter("msg_errorReqEstCostNonRenewal")[0]
MSG_ERROR_REQESTCOST_RENEWAL150 = fn_get_parameter("msg_errorReqEstCostRenewal150")[0]
MSG_ERROR_REQIMPACT = fn_get_parameter("msg_errorReqImpact")[0]
MSG_ERROR_REQCATEGORY = fn_get_parameter("msg_errorReqCategory")[0]
MSG_ERROR_REQCATEGORY_RENEWAL = fn_get_parameter("msg_errorReqCategoryRenewal")[0]
MSG_ERROR_REQPRIORITY = fn_get_parameter("msg_errorReqPriority")[0]
MSG_ERROR_REQUNIFORMAT_MISSING = fn_get_parameter("msg_errorReqUniformatMissing")[0]
MSG_ERROR_REQUNIFORMAT_NOT_CUSTOM = fn_get_parameter("msg_errorReqUniformatNotCustom")[0]
MSG_ERROR_REQSYSGRP_MISMATCH = fn_get_parameter("msg_errorReqSysGrpMismatch")[0]
MSG_ERROR_REQSYSGRP_MISSING = fn_get_parameter("msg_errorReqSysGrpMissing")[0]
MSG_ERROR_MISSINGSCORECRITERIA_VALUE = fn_get_parameter("msg_errorProjSummaryScoreCriteria")[0]
MSG_ERROR_REQNAMEBLANK = fn_get_parameter("msg_errorReqName")[0]
MSG_ERROR_REQDESCBLANK = fn_get_parameter("msg_errorReqDesc")[0]
MSG_ERROR_PROJSUMRY_RAPDATA_MISSING = fn_get_parameter("msg_errorProjSummayRAPData")[0]
MSG_ERROR_RAPDESCBLANK = fn_get_parameter("msg_errorRAPDesc")[0]
MSG_ERROR_RAPCONSBLANK = fn_get_parameter("msg_errorRAPConsequence")[0]
MSG_ERROR_RAPREASONBLANK = fn_get_parameter("msg_errorRAPReason")[0]
MSG_ERROR_RAPDATAVALIDATION = fn_get_parameter("msg_errorRAPDataValidation")[0]

#-- WARNING MESSAGES -- 
MSG_WARN_REQESTCOST = fn_get_parameter("msg_warnReqEstCost")[0]
MSG_WARN_REQPRIORITY_RENEWAL = fn_get_parameter("msg_warnReqPriorityRenewal")[0]
MSG_WARN_REQPRIORITY_NONRENEWAL = fn_get_parameter("msg_warnReqPriorityNonRenewal")[0]
MSG_WARN_REQNAME = fn_get_parameter("msg_warnReqName")[0]
MSG_WARN_REQDESC = fn_get_parameter("msg_warnReqDesc")[0]
MSG_WARN_RAPTITLELENGTH = fn_get_parameter("msg_warnRAPTitleLength")[0]

#--INFORMATIONAL MESSAGES --
MSG_INFO_NOLINKEDREQ_MESSAGE = fn_get_parameter("msg_infoRAPNoLinkedReq")[0]
MSG_INFO_RAPSTATUSCOM = fn_get_parameter("msg_infoRAPStatusCOM")[0]
MSG_INFO_UNIFORMATEXCEPTION = fn_get_parameter("msg_infoReqUniformatException")[0]
MSG_INFO_SYSTEMGROUPEXCEPTION = fn_get_parameter("msg_infoReqSysGrpValidException")[0]
MSG_INFO_REQDATAVALIDATION_MESSAGE = fn_get_parameter("msg_infoReqDataValidationMsg")[0]
MSG_INFO_RAPDATAVALIDATION_MESSAGE = fn_get_parameter("msg_infoRAPDataValidation")[0]
MSG_INFO_RAPDESCVALID_MESSAGE = fn_get_parameter("msg_infoRAPDescValidation")[0]
MSG_INFO_RAPCONSVALID_MESSAGE = fn_get_parameter("msg_infoRAPConsValidation")[0]
MSG_INFO_RAPREASONVALID_MESSAGE = fn_get_parameter("msg_infoRAPReasonValidation")[0]

# --- 2. Combined Planning Data Preparation ---
df_2025 = annual_planner_b2025_df.select(
    F.col("Asset Number").alias("AssetID"), 
    F.col("REQ ID").alias("REQID")
).withColumn("PlannedStatus", F.lit("Planned"))

df_2026 = annual_planner_b2026_df.select(
    F.col("Asset Number").alias("AssetID"), 
    F.col("REQ ID").alias("REQID")
).withColumn("PlannedStatus", F.lit("Planned"))

# UNION AND DEDUPLICATE
annual_planner_combined_df = df_2025.unionByName(df_2026) \
    .groupBy("AssetID", "REQID") \
    .agg(F.max(F.col("PlannedStatus")).alias("PlannedStatus"))

# --- 3. VFA Staging Function Joins on RAPID in both source tables ---
def vfa_ssrs_staging(vfa_df, rap_df):
    # Select RAPID only, removing duplicates (Comments removed from this stage as requested)
    rap_data = rap_df.select("RAPID").distinct()
    df = vfa_df.join(rap_data, ["RAPID"], "inner")
    final_cols = ["RAPID", "RAPStatus", "PortfolioName", "AssetName", "AssetID", "AssetUse", "BuildingPriorityDetail", "REQID", "RequirementName", "RequirementDescription", "Category", "Priority", "Impact", "EstimatedCost", "SystemName", "Uniformat", "VFASystemGroup", "System_EID", "OutForAudit", "IsRenewal", "Location"]
    return df.select(*final_cols)

vfa_ssrs_staged_df = vfa_ssrs_staging(vfa_ssrs_raw_df, rap_ssrs_df)


# --- 4. CMR Scoring Model (Validation and Planned Status Integration) ---
def score_and_validate_requirements(source_df, planner_df):
    df = source_df
    
    # --- 1. JOINS ---
    # --- a. IoSF Score and Master data check with Uniformat as join key, returning IOSF_Score, MasterUniformat, and MasterSystemGroup ---
    # NOTE: We rename to MasterUniformat here so we can keep it after the join to check for mismatches.
    iosf_score = iosf_df.select(
        F.col("System - Uniformat").alias("MasterUniformat"), 
        F.col("Importance of System Functionality").alias("IoSF"), 
        F.col("Score with Weight").alias("IoSF_Score"), 
        F.col("System Group").alias("MasterSystemGroup")
    )
    df = df.join(iosf_score, df.Uniformat == iosf_score.MasterUniformat, "left_outer")
    
    # --- b. Plan for Asset with Building Priority Detail as join key, returning PFA_Score ---
    pfa_score = planforasset_df.select(F.col("Building Priority Detail").alias("Join_Key"), F.col("Plan for Asset").alias("PlanForAsset"), F.col("Score with Weight").alias("PFA_Score"))
    df = df.join(pfa_score, df.BuildingPriorityDetail == pfa_score.Join_Key, "left_outer").drop("Join_Key")
    # --- c. Importance of Asset Functionality with Asset Use as join key, returning IOAF_Score
    ioaf_score = ioaf_df.select(F.col("Asset Use").alias("Join_Key"), F.col("Importance of Asset Functionality").alias("IoAF"), F.col("Score with Weight").alias("IoAF_Score"))
    df = df.join(ioaf_score, df.AssetUse == ioaf_score.Join_Key, "left_outer").drop("Join_Key")
    # --- d. Condition of System with Priority as join key, returning Condition_Score 
    cos_score = conditionofsystem_df.select(F.col("Requirement Priority").alias("Join_Key"), F.col("Condition of System").alias("ConditionofSystem"), F.col("Score with Weight").alias("Condition_Score"))
    df = df.join(cos_score, df.Priority == cos_score.Join_Key, "left_outer").drop("Join_Key")
    # --- e. Reason for Work with Category as join key, returning Reason_Score
    rfw_score = reasonforwork_df.select(F.col("Requirement Category").alias("Join_Key"), F.col("Reason for Work").alias("ReasonforWork"), F.col("Score with Weight").alias("Reason_Score"))
    df = df.join(rfw_score, df.Category == rfw_score.Join_Key, "left_outer").drop("Join_Key")
    # --- f. Impact of Failure with Impact as join key, returning Impact_Score ---
    iof_score = impactoffailure_df.select(F.col("Requirement Impact").alias("Join_Key"), F.col("Impact of Failure").alias("ImpactofFailure"), F.col("Score with Weight").alias("Impact_Score"))
    df = df.join(iof_score, df.Impact == iof_score.Join_Key, "left_outer").drop("Join_Key")

    # --- 2. SCORING ---
    score_sum = (
        F.coalesce(F.col("PFA_Score"), F.lit(0)) + 
        F.coalesce(F.col("IoAF_Score"), F.lit(0)) + 
        F.coalesce(F.col("IoSF_Score"), F.lit(0)) + 
        F.coalesce(F.col("Condition_Score"), F.lit(0)) + 
        F.coalesce(F.col("Reason_Score"), F.lit(0)) + 
        F.coalesce(F.col("Impact_Score"), F.lit(0))
    )
    df = df.withColumn("PriorityScore", F.round(score_sum, 0).cast(LongType()).cast(StringType()))
    # NO CAST TO LONG HERE - Keep EstimatedCost as numeric/double for now to preserve precision
    # df = df.withColumn("EstimatedCost", F.round(F.col("EstimatedCost"), 0).cast(LongType()))

    # --- 3. DATA VALIDATION SETUP ---
    is_renewal_safe = F.coalesce(F.col("IsRenewal"), F.lit(False))
    est_cost_safe = F.coalesce(F.col("EstimatedCost"), F.lit(0))

    # --- HELPER: LOGIC APPLIER ---
    def apply_validation_logic(df, check_col, level_col, pass_msg, rules):
        check_expr = None
        level_expr = None
        for condition, message, level in rules:
            if check_expr is None:
                check_expr = F.when(condition, message)
                level_expr = F.when(condition, F.lit(level).cast(LongType()))
            else:
                check_expr = check_expr.when(condition, message)
                level_expr = level_expr.when(condition, F.lit(level).cast(LongType()))
        
        if check_expr is None: 
            check_expr = F.lit(pass_msg)
            level_expr = F.lit(None).cast(LongType())
        else:
            check_expr = check_expr.otherwise(F.lit(pass_msg))
            level_expr = level_expr.otherwise(F.lit(None).cast(LongType()))

        return df.withColumn(check_col, check_expr).withColumn(level_col, level_expr)

    # --- PREPARE DYNAMIC MESSAGES ---
    def get_dynamic_msg(template, placeholder, value_col):
        if template and placeholder in template:
            clean_fmt = template.replace(placeholder, "%s")
            return F.format_string(clean_fmt, value_col)
        else:
            return F.lit(template)

    # Updated "Unknown" to "Error" for clarity
    msg_sys_missing = get_dynamic_msg(MSG_ERROR_REQSYSGRP_MISSING, "{MasterSystemGroup}", F.coalesce(F.col("MasterSystemGroup"), F.lit("Error")))
    msg_cost_warn = get_dynamic_msg(MSG_WARN_REQESTCOST, "{Limit}", F.lit(WARN_COST_VAL).cast(LongType()))
    
    if "{MasterSystemGroup}" in MSG_ERROR_REQSYSGRP_MISMATCH:
        msg_sys_mismatch = get_dynamic_msg(MSG_ERROR_REQSYSGRP_MISMATCH, "{MasterSystemGroup}", F.coalesce(F.col("MasterSystemGroup"), F.lit("Error")))
    else:
        msg_sys_mismatch = F.concat(F.lit(MSG_ERROR_REQSYSGRP_MISMATCH), F.lit(" (Expected: "), F.coalesce(F.col("MasterSystemGroup"), F.lit("Error")), F.lit(")"))

    # --- RULES ---
    cost_rules = [
        ((est_cost_safe == 0) & (is_renewal_safe == True), F.lit(MSG_ERROR_REQESTCOST_RENEWAL), 1),
        ((est_cost_safe == 0) & (is_renewal_safe == False), F.lit(MSG_ERROR_REQESTCOST_NONRENEWAL), 1),
        ((est_cost_safe < 150) & (is_renewal_safe == True), F.lit(MSG_ERROR_REQESTCOST_RENEWAL150), 1),
        (F.col("EstimatedCost").isNotNull() & (F.col("EstimatedCost") > WARN_COST_VAL), msg_cost_warn, 2)
    ]
    df = apply_validation_logic(df, "EstCostCheck", "EstCostErrorLevel", MSG_PASS_REQESTCOST, cost_rules)

    # --- UNIFORMAT RULES (UPDATED TO MATCH POWER QUERY LOGIC) ---
    
    # 1. RAP COM & No System: RAP Status is COM and System_EID is blank/null
    cond_rap_com_no_system = (
        (F.col("RAPStatus") == "COM") & 
        ((F.col("System_EID") == "") | F.col("System_EID").isNull())
    )

    # 2. Missing Uniformat (Non-Renewal): Uniformat is Null, Not Renewal, Not RAP COM
    cond_missing_uniformat = (
        F.col("Uniformat").isNull() & 
        (is_renewal_safe == False) & 
        (F.col("RAPStatus") != "COM")
    )

    # 3. RAP COM & All Nulls: Uniformat missing, Master Data missing, but RAP is COM (Info only)
    cond_rap_com_all_null = (
        F.col("Uniformat").isNull() & 
        F.col("MasterSystemGroup").isNull() & 
        F.col("MasterUniformat").isNull() & 
        (F.col("RAPStatus") == "COM")
    )

    # 4. Mismatch (The "Real" Error): Uniformat exists, but Master Data is missing (Wrong Template)
    cond_mismatch = (
        F.col("Uniformat").isNotNull() & 
        F.col("MasterUniformat").isNull() & 
        F.col("MasterSystemGroup").isNull() & 
        (F.col("RAPStatus") != "COM")
    )

    uniformat_rules = [
        (cond_rap_com_no_system, F.lit(MSG_INFO_RAPSTATUSCOM), 0),
        (cond_missing_uniformat, F.lit(MSG_ERROR_REQUNIFORMAT_MISSING), 1),
        (cond_rap_com_all_null, F.lit(MSG_INFO_RAPSTATUSCOM), 0),
        (cond_mismatch, F.lit(MSG_ERROR_REQUNIFORMAT_NOT_CUSTOM), 1)
    ]
    df = apply_validation_logic(df, "UniformatCheck", "UniformatErrorLevel", MSG_PASS_REQUNIFORMAT, uniformat_rules)

    # --- SYSTEM GROUP RULES ---
    
    # Check for F20 (Demolition) exception
    cond_f20_exception = F.col("Uniformat").startswith("F20")

    sysgrp_rules = [
        ((F.col("RAPStatus") == "COM") & F.col("System_EID").isNull(), F.lit(MSG_INFO_SYSTEMGROUPEXCEPTION), 0),
        (cond_f20_exception, F.lit(MSG_INFO_SYSTEMGROUPEXCEPTION), 0), # Added F20 Exception
        (F.col("VFASystemGroup").isNull() & F.col("Uniformat").isNotNull(), msg_sys_missing, 1),
        (F.col("VFASystemGroup").isNotNull() & (F.col("VFASystemGroup") != F.col("MasterSystemGroup")), msg_sys_mismatch, 1)
    ]
    df = apply_validation_logic(df, "SystemGroupCheck", "SystemGroupErrorLevel", MSG_PASS_REQSYSGRP, sysgrp_rules)

    category_rules = [
        (F.col("Category").isNull(), F.lit(MSG_ERROR_REQCATEGORY), 1),
        ((is_renewal_safe == True) & (F.col("Category") != "Lifecycle"), F.lit(MSG_ERROR_REQCATEGORY_RENEWAL), 1)
    ]
    df = apply_validation_logic(df, "CategoryCheck", "CategoryErrorLevel", MSG_PASS_REQCATEGORY, category_rules)

    priority_rules = [
        (F.col("Priority").isNull(), F.lit(MSG_ERROR_REQPRIORITY), 1),
        ((is_renewal_safe == True) & (F.col("Priority") == "Lifecycle Planning (at least 4 years remaining at inspection)"), F.lit(MSG_WARN_REQPRIORITY_RENEWAL), 2),
        ((is_renewal_safe == False) & (F.col("Priority") == "Lifecycle Planning (at least 4 years remaining at inspection)"), F.lit(MSG_WARN_REQPRIORITY_NONRENEWAL), 2)
    ]
    df = apply_validation_logic(df, "PriorityCheck", "PriorityErrorLevel", MSG_PASS_REQPRIORITY, priority_rules)

    impact_rules = [(F.col("Impact").isNull(), F.lit(MSG_ERROR_REQIMPACT), 1)]
    df = apply_validation_logic(df, "ImpactCheck", "ImpactErrorLevel", MSG_PASS_REQIMPACT, impact_rules)

    name_rules = [
        (F.col("RequirementName").isNull() | (F.trim(F.col("RequirementName")) == ""), F.lit(MSG_ERROR_REQNAMEBLANK), 1),
        (F.trim(F.col("RequirementName")).startswith("~"), F.lit(MSG_WARN_REQNAME), 2)
    ]
    df = apply_validation_logic(df, "ReqNameCheck", "ReqNameErrorLevel", MSG_PASS_REQNAME, name_rules)

    desc_rules = [
        (F.col("RequirementDescription").isNull() | (F.trim(F.col("RequirementDescription")) == ""), F.lit(MSG_ERROR_REQDESCBLANK), 1),
        (F.length(F.col("RequirementDescription")) > 4000, F.lit(MSG_WARN_REQDESC), 2)
    ]
    df = apply_validation_logic(df, "ReqDescCheck", "ReqDescErrorLevel", MSG_PASS_REQDESC, desc_rules)

    # --- 4. INTEGRATE PLANNED STATUS ---
    df = df.join(planner_df, ["AssetID", "REQID"], "left_outer")

    # --- 5. FINAL ISSUE COUNT ---
    error_columns = ["EstCostErrorLevel", "UniformatErrorLevel", "SystemGroupErrorLevel", "ImpactErrorLevel", "CategoryErrorLevel", "PriorityErrorLevel", "ReqNameErrorLevel", "ReqDescErrorLevel"]
    
    issue_expr = F.lit(0)
    for col in error_columns:
        issue_expr = issue_expr + F.when(F.col(col).isin(1, 2), 1).otherwise(0)

    df = df.withColumn("Issues_Count", issue_expr)
    df = df.withColumn("VFADataValidationTotalIssues", F.col("Issues_Count").cast(StringType()))
    
    df = df.withColumn("VFADataValidationValidationMessage", 
        F.when(F.col("Issues_Count") > 0, F.lit(MSG_INFO_REQDATAVALIDATION_MESSAGE))
         .otherwise(F.lit(None).cast(StringType()))
    )

    return df.select(
        F.col("*"),
        F.col("IoSF_Score").alias("IoSFScore"), 
        F.col("PFA_Score").alias("PFAScore"), 
        F.col("IoAF_Score").alias("IoAFScore"),
        F.col("Condition_Score").alias("ConditionScore"),
        F.col("Reason_Score").alias("ReasonScore"),
        F.col("Impact_Score").alias("ImpactScore")
    ).drop("IoSF_Score", "PFA_Score", "IoAF_Score", "Condition_Score", "Reason_Score", "Impact_Score", "Issues_Count", "SystemGrpErrorCount", "CategoryWarnCount")

# --- RE-BIND THE DATAFRAME ---
vfa_data_validation_df = score_and_validate_requirements(vfa_ssrs_staged_df, annual_planner_combined_df)

# --- 5. Aggregation Functions ---

def cost_factor_model(source_df):
    df = source_df.select("RAPID", "EstimatedCost", "Category")
    
    # Note: We use DoubleType for intermediate calculations to avoid rounding errors
    is_study = F.col("Category").contains("_Study [Non-FCI]")
    
    # ROW LEVEL LOGIC (Matching Legacy Power Query "If < 1000 use 1000" rule)
    # UPDATED: Removed the explicit "0 stays 0" check. 
    # Legacy logic treats 0 as < 1000, applying the $1000 minimum floor.
    df = df.withColumn("Row_Factored_Cost", 
        F.when(is_study, 
            F.ceil(F.col("EstimatedCost").cast(DoubleType()) / 100) * 100
        ).otherwise(
            # Non-Study: If Cost < 1000 (including 0), use 1000. Else, apply factor and round up.
            F.when(F.col("EstimatedCost") < 1000, F.lit(1000).cast(LongType()))
             .otherwise(F.ceil((F.col("EstimatedCost").cast(DoubleType()) * 1.48) / 1000) * 1000)
        ).cast(LongType())
    )

    # 4. Sum the already-rounded rows
    grouped_df = df.groupBy("RAPID").agg(
        F.sum("EstimatedCost").cast(LongType()).alias("EstimatedTPC"), 
        F.sum("Row_Factored_Cost").cast(LongType()).alias("CostFactoredTPC")
    )
    
    # CASH FLOW (Rounding to 100 and Balancing)
    grouped_df = grouped_df.withColumn("Y1_Raw", 
        F.when(F.col("CostFactoredTPC") < 500000, F.col("CostFactoredTPC"))
         .otherwise(F.when(F.ceil((F.col("CostFactoredTPC") * 0.08) / 100) * 100 > 5000000, 5000000).otherwise(F.ceil((F.col("CostFactoredTPC") * 0.08) / 100) * 100))
         .cast(LongType())
    )

    y2_calc = F.ceil((F.col("CostFactoredTPC") * 0.62) / 100) * 100
    y2_capped = F.when(y2_calc > 5000000, 5000000).otherwise(y2_calc)
    grouped_df = grouped_df.withColumn("Y2_Raw", 
        F.when(F.col("CostFactoredTPC") < 500000, F.lit(0))
         .when(F.col("CostFactoredTPC") <= 2000000, F.col("CostFactoredTPC") - F.col("Y1_Raw"))
         .otherwise(y2_capped)
         .cast(LongType())
    )

    y3_calc = F.ceil((F.col("CostFactoredTPC") * 0.30) / 100) * 100
    y3_capped = F.when(y3_calc > 5000000, 5000000).otherwise(y3_calc)
    grouped_df = grouped_df.withColumn("Y3_Raw", 
        F.when(F.col("CostFactoredTPC") <= 2000000, F.lit(0))
         .otherwise(y3_capped)
         .cast(LongType())
    )
    
    grouped_df = grouped_df.withColumn("Allocated_So_Far", F.col("Y1_Raw") + F.col("Y2_Raw") + F.col("Y3_Raw"))
    
    grouped_df = grouped_df.select(
        "RAPID", "EstimatedTPC", "CostFactoredTPC",
        F.col("Y1_Raw").alias("CashFlowYear1"),
        F.col("Y2_Raw").alias("CashFlowYear2"),
        F.when(F.col("Allocated_So_Far") > F.col("CostFactoredTPC"), F.col("Y3_Raw") - (F.col("Allocated_So_Far") - F.col("CostFactoredTPC")))
         .otherwise(F.col("Y3_Raw")).cast(LongType()).alias("CashFlowYear3"),
        F.when(F.col("Allocated_So_Far") < F.col("CostFactoredTPC"), F.col("CostFactoredTPC") - F.col("Allocated_So_Far"))
         .otherwise(F.lit(0)).cast(LongType()).alias("FutureYears")
    )
    return grouped_df

cost_factor_df = cost_factor_model(vfa_data_validation_df)

def project_score_breakdown(source_df):
    window_spec = Window.partitionBy("RAPID").orderBy(
        F.col("PriorityScore").cast(DoubleType()).desc(),
        #F.col("ReasonScore").cast(DoubleType()).desc(), # Tie-Breaker: High Severity Category wins
        #F.col("EstimatedCost").desc(),                  # Tie-Breaker: High Cost wins
        F.col("REQID").desc()                           # Tie-Breaker: Deterministic ID sort
    )
    ranked_df = source_df.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") == 1)
    
    return ranked_df.select(
        "RAPID", F.col("PriorityScore").alias("ProjectPriorityScore"), F.col("REQID").alias("HighScoreReq"),
        "PlanForAsset", "BuildingPriorityDetail", F.col("PFAScore"), "IoAF", F.col("IoAFScore"), 
        "AssetUse", "Uniformat", "IoSF", F.col("IoSFScore"), "ConditionofSystem", F.col("ConditionScore"),
        "Priority", "ReasonforWork", F.col("ReasonScore"), "Category", "ImpactofFailure", F.col("ImpactScore"), "Impact"
    )

project_score_breakdown_df = project_score_breakdown(vfa_data_validation_df)

def vfa_project_staging(source_df):
    def count_err(col_name): return F.when(F.col(col_name) == 1, 1).otherwise(0)

    grouped_df = source_df.groupBy("RAPID").agg(
        F.concat_ws(", ", F.collect_set(F.col("PortfolioName"))).alias("Portfolio_Name_s"), 
        F.concat_ws(", ", F.collect_set(F.col("AssetID"))).alias("Asset_ID_s"), 
        F.concat_ws(", ", F.collect_set(F.col("AssetName"))).alias("Asset_Name_s"), 
        F.concat_ws(", ", F.collect_set(F.col("AssetUse"))).alias("Asset_Use_s"), 
        F.concat_ws(", ", F.collect_set(F.col("BuildingPriorityDetail"))).alias("Building_Priority_Detail_s"), 
        F.concat_ws(", ", F.collect_set(F.col("REQID"))).alias("REQ_ID_s"), 
        F.concat_ws("; ", F.collect_set(F.col("RequirementName"))).alias("Requirement_Name_s"), 
        F.concat_ws("; ", F.collect_set(F.col("Uniformat"))).alias("Linked_System_s"), 
        F.concat_ws("; ", F.collect_set(F.col("VFASystemGroup"))).alias("System_Groups_s"), 
        # TRUNCATE DESCRIPTION TO 4000 CHARS (Aggregated string)
        F.substring(F.concat_ws("; ", F.collect_set(F.col("RequirementDescription"))), 1, 4000).alias("Requirement_Description_s"), 
        F.concat_ws(", ", F.collect_set(F.col("Location"))).alias("Location_s"),
        
        F.sum(count_err("ImpactErrorLevel") + count_err("CategoryErrorLevel") + count_err("PriorityErrorLevel") + count_err("UniformatErrorLevel")).alias("VFA_Scoring_Error_s"),
        F.sum(count_err("ReqNameErrorLevel") + count_err("ReqDescErrorLevel")).alias("VFA_Scoping_Error_s"),
        F.sum(count_err("SystemGroupErrorLevel")).alias("VFA_Maintenance_Planning_Error_s"),
        F.sum(count_err("EstCostErrorLevel")).alias("VFA_Costing_Error_s"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("PlannedStatus")))).alias("Planned_Status_Raw"),
        F.max(F.col("IsRenewal").cast(LongType())).alias("HasRenewal")
    )
    
    grouped_df = grouped_df.withColumn("Planned_Status", F.when(F.col("Planned_Status_Raw") == "", F.lit("Unplanned")).otherwise(F.col("Planned_Status_Raw"))).drop("Planned_Status_Raw")
    grouped_df = grouped_df.withColumn("Project_Includes_Renewal_Requirements", F.when(F.col("HasRenewal") == 1, F.lit("Yes")).otherwise(F.lit("No"))).drop("HasRenewal")
    grouped_df = grouped_df.withColumn("Asset_Out_For_Audit", F.lit("No"))
    return grouped_df

vfa_project_staging_df = vfa_project_staging(vfa_data_validation_df)

def project_request_summary(rap_df, score_df, vfa_project_df, cost_df):
    vfa_project_df_clean = vfa_project_df.select("RAPID", "Portfolio_Name_s", "Asset_ID_s", "Asset_Name_s", "Asset_Use_s", "Building_Priority_Detail_s", "REQ_ID_s", "Requirement_Name_s", "Linked_System_s", "System_Groups_s", "Requirement_Description_s", "Location_s", "VFA_Scoring_Error_s", "VFA_Scoping_Error_s", "VFA_Maintenance_Planning_Error_s", "VFA_Costing_Error_s", "Project_Includes_Renewal_Requirements", "Asset_Out_For_Audit", "Planned_Status")
    
    df = rap_df.withColumnRenamed("RAPStatus", "RAPStatus_RAPSSRS")
    df = df.join(score_df, ["RAPID"], "left_outer").join(vfa_project_df_clean, ["RAPID"], "left_outer").join(cost_df, ["RAPID"], "left_outer")
    
    df = df.withColumn("VFA_Data_Flag", F.col("ProjectPriorityScore").isNull())
    df = df.withColumn("CostWarning", F.when(F.col("VFA_Data_Flag"), F.lit(None).cast(StringType())).when(F.col("CostFactoredTPC") > 5000000, F.lit(PROJECT_WARN_COSTMSG)).otherwise(F.lit(PROJECT_INFO_COSTMSG)))
    
    df = df.withColumn("RAPDescCheck", F.when(F.col("RAPProjectDescription").isNull() | (F.trim(F.col("RAPProjectDescription")) == ""), F.lit(MSG_ERROR_RAPDESCBLANK)).otherwise(F.lit(MSG_PASS_RAPDESC)))
    df = df.withColumn("RAPConseqCheck", F.when(F.col("RAPProjectConsequence").isNull() | (F.trim(F.col("RAPProjectConsequence")) == ""), F.lit(MSG_ERROR_RAPCONSBLANK)).otherwise(F.lit(MSG_PASS_RAPCONS)))
    df = df.withColumn("RAPReasonCheck", F.when(F.col("RAPProjectReason").isNull() | (F.trim(F.col("RAPProjectReason")) == ""), F.lit(MSG_ERROR_RAPREASONBLANK)).otherwise(F.lit(MSG_PASS_RAPREASON)))
    # Changed fallback from "--" to "0"
    df = df.withColumn("RAPTitleLengthCheck", F.when(F.length(F.col("RAPTitle")) > 40, F.lit(MSG_WARN_RAPTITLELENGTH)).otherwise(F.lit("--")))
    
    # Calculate Issue Count (Matches VFA Validation Logic: Sum of Errors ONLY, excluding Title Length Warning)
    rap_issue_count = (
        F.when(F.col("RAPDescCheck") == F.lit(MSG_ERROR_RAPDESCBLANK), 1).otherwise(0) +
        F.when(F.col("RAPConseqCheck") == F.lit(MSG_ERROR_RAPCONSBLANK), 1).otherwise(0) +
        F.when(F.col("RAPReasonCheck") == F.lit(MSG_ERROR_RAPREASONBLANK), 1).otherwise(0)
    )

    # Use the count to determine the message
    df = df.withColumn("RAPDataValidation", 
        F.when(rap_issue_count > 0, rap_issue_count.cast(StringType()))
         .otherwise(F.lit(MSG_INFO_RAPDATAVALIDATION_MESSAGE))
    )

    df = df.withColumn("ProjectPriorityScore", F.when(F.col("VFA_Data_Flag"), F.lit(MSG_INFO_NOLINKEDREQ_MESSAGE)).otherwise(F.col("ProjectPriorityScore")))
    df = df.drop("VFA_Data_Flag").withColumnRenamed("RAPStatus_RAPSSRS", "RAPStatus")
    
    final_output_cols = ["RAPID", "RAPTitle", "RAPStatus", "RAPProjectDescription", "RAPProjectReason", "RAPProjectConsequence", "RAPProjectComments", "RAPTitleLengthCheck", "RAPDescCheck", "RAPConseqCheck", "RAPReasonCheck", "RAPDataValidation", "ProjectPriorityScore", "HighScoreReq", "PlanForAsset", "BuildingPriorityDetail", "PFAScore", "IoAF", "IoAFScore", "AssetUse", "Uniformat", "IoSF", "IoSFScore", "ConditionofSystem", "ConditionScore", "Priority", "ReasonForWork", "ReasonScore", "Category", "ImpactofFailure", "ImpactScore", "Impact", "VFA_Scoring_Error_s", "VFA_Scoping_Error_s", "VFA_Maintenance_Planning_Error_s", "VFA_Costing_Error_s", "Portfolio_Name_s", "Asset_ID_s", "Asset_Name_s", "Asset_Use_s", "Building_Priority_Detail_s", "REQ_ID_s", "Requirement_Name_s", "Linked_System_s", "System_Groups_s", "Requirement_Description_s", "Location_s", "Project_Includes_Renewal_Requirements", "Asset_Out_For_Audit", "Planned_Status", "EstimatedTPC", "CostFactoredTPC", "CostWarning", "CashFlowYear1", "CashFlowYear2", "CashFlowYear3", "FutureYears"]
    return df.select(*final_output_cols)

project_summary_data_df = project_request_summary(rap_ssrs_df, project_score_breakdown_df, vfa_project_staging_df, cost_factor_df)

# --- Save to Lakehouse Tables ---
# Cast EstimatedCost to LongType right before saving to match expected schema
vfa_data_validation_df.withColumn("EstimatedCost", F.col("EstimatedCost").cast(LongType())) \
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("VFADataValidation")
project_summary_data_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("ProjectSummaryData")

print("\n✅ Final PySpark Pipeline Execution Complete!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- 1. SETUP ---
new_df = spark.table("LH_PROD_CP_CMR.projectsummarydata")
old_df = spark.table("LH_DEV_CP_CMR.pmbcp_projectsummarydata_prod") # <--- REPLACE THIS WITH ACTUAL TABLE NAME

# --- 2. DEFINE THE MAPPING ---
# Key = New PySpark Column Name
# Value = Old Dataverse Column Name
column_mapping = {
    # -- Identifiers --
    "RAPID": "pmbcp_rapid",
    "RAPStatus": "pmbcp_rapstatus",
    "RAPTitle": "pmbcp_title",
    
    # -- RAP Data --
    "RAPProjectDescription": "pmbcp_projectdescription",
    "RAPProjectReason": "pmbcp_projectreason",
    "RAPProjectConsequence": "pmbcp_projectconsequence",
    "RAPProjectComments": "pmbcp_projectcomments",
    
    # -- RAP Validation --
    "RAPTitleLengthCheck": "pmbcp_titlelengthvalidation",
    "RAPDescCheck": "pmbcp_descriptionvalidation",
    "RAPConseqCheck": "pmbcp_consequencevalidation",
    "RAPReasonCheck": "pmbcp_reasonvalidation",
    "RAPDataValidation": "pmbcp_rapdatavalidation",
    
    # -- Scores & Attributes --
    "ProjectPriorityScore": "pmbcp_projectpriorityscore",
    "HighScoreReq": "pmbcp_highestrequirementid",
    "PlanForAsset": "pmbcp_assetplan",
    "PFAScore": "pmbcp_assetplanscore",
    "IoAF": "pmbcp_assetfunctionalityimportance",
    "IoAFScore": "pmbcp_assetfunctionalityscore",
    "AssetUse": "pmbcp_assetuse",
    "Uniformat": "pmbcp_linkedsystemrequirement",
    "IoSF": "pmbcp_systemfunctionalityimportance",
    "IoSFScore": "pmbcp_systemfunctionalityscore",
    "ConditionofSystem": "pmbcp_systemcondition",
    "ConditionScore": "pmbcp_systemconditionscore",
    "Priority": "pmbcp_priority",
    "ReasonforWork": "pmbcp_workreason",
    "ReasonScore": "pmbcp_workreasonscore",
    "Category": "pmbcp_category",
    "ImpactofFailure": "pmbcp_failureimpact",
    "ImpactScore": "pmbcp_failureimpactscore",
    "Impact": "pmbcp_impact",
    
    # -- Aggregated Strings --
    "Portfolio_Name_s": "pmbcp_portfolionames",
    "Asset_ID_s": "pmbcp_assetnumbers",
    "Asset_Name_s": "pmbcp_assetnames",
    "Asset_Use_s": "pmbcp_assetuses",
    "Building_Priority_Detail_s": "pmbcp_buildingpriority",
    "REQ_ID_s": "pmbcp_requirementids",
    "Requirement_Name_s": "pmbcp_projectsummary",
    "Linked_System_s": "pmbcp_linkedsystems",
    "System_Groups_s": "pmbcp_systemgroups",
    "Requirement_Description_s": "pmbcp_vfaprojectdescription",
    "Location_s": "pmbcp_locations",
    
    # -- Error Counts --
    "VFA_Scoring_Error_s": "pmbcp_vfascoringerrors",
    "VFA_Scoping_Error_s": "pmbcp_vfascopingerrors",
    "VFA_Maintenance_Planning_Error_s": "pmbcp_vfamaintplanerrors",
    "VFA_Costing_Error_s": "pmbcp_vfacostingerrors",
    
    # -- Costs & Status --
    "EstimatedTPC": "pmbcp_estimatedcost",
    "CostFactoredTPC": "pmbcp_costtpc",
    "CostWarning": "pmbcp_vfacostingwarning",
    "Planned_Status": "pmbcp_plannedstatus",
    "Project_Includes_Renewal_Requirements": "pmbcp_renewalrequirements",
    "Asset_Out_For_Audit": "pmbcp_assetoutforaudit"
}

# --- 3. PREPARE COMPARISON ---
old_select_expr = [F.col("pmbcp_rapid").alias("RAPID")]
for new_col, old_col in column_mapping.items():
    if new_col != "RAPID":
        old_select_expr.append(F.col(old_col).alias(f"{new_col}_old"))

old_subset = old_df.select(*old_select_expr)
comparison_df = new_df.join(old_subset, "RAPID", "inner")

# --- 4. RUN COMPARISON ---
diff_filter = F.lit(False)
failed_col_exprs = [] # <-- List to store names of failed columns

print("--- Checking for Mismatches (Ignoring Blank vs Null differences) ---")

for col in column_mapping.keys():
    if col == "RAPID": continue
    
    new_val = F.col(col)
    old_val = F.col(f"{col}_old")
    
    # Base Transformation
    new_val_str = F.trim(new_val.cast("string"))
    old_val_str = F.trim(old_val.cast("string"))

    # SPECIAL HANDLING: Costs (Ignore Decimals: 100 vs 100.00)
    if col in ["EstimatedTPC", "CostFactoredTPC"]:
        new_val_str = F.round(new_val.cast("double"), 0).cast("long").cast("string")
        old_val_str = F.round(old_val.cast("double"), 0).cast("long").cast("string")

    # LOGIC: Coalesce(Trim(Val), "") converts Nulls and Spaces to empty strings
    is_different = (
        F.coalesce(new_val_str, F.lit("")) != 
        F.coalesce(old_val_str, F.lit(""))
    )
    
    comparison_df = comparison_df.withColumn(f"Diff_{col}", is_different)
    diff_filter = diff_filter | is_different
    
    # If different, return the column name, otherwise null
    failed_col_exprs.append(F.when(is_different, F.lit(col)).otherwise(F.lit(None)))

# --- CREATE SUMMARY COLUMN ---
# Concatenate all failed column names into a comma-separated string
comparison_df = comparison_df.withColumn("Failed_Columns", F.concat_ws(", ", *failed_col_exprs))

# --- 5. REPORT RESULTS ---
mismatches = comparison_df.filter(diff_filter)
mismatches.cache()

total_count = mismatches.count()

print(f"\nProject Summary Total Rows with Discrepancies: {total_count}")

# Safe display helper for non-notebook environments
def safe_display(df, limit=40):
    try:
        display(df.limit(limit))
    except NameError:
        df.show(limit, truncate=False)

if total_count > 0:
    print("\nTop 40 Mismatches (Showing New vs Old):")
    
    # Start display with RAPID and the new Failed_Columns list
    display_cols = ["RAPID", "Failed_Columns"]
    
    for col in column_mapping.keys():
        if col == "RAPID": continue
        # Only include detailed columns if they actually have a failure in this subset
        if mismatches.filter(F.col(f"Diff_{col}") == True).count() > 0:
            display_cols.extend([col, f"{col}_old"])
            
    safe_display(mismatches.select(*display_cols))
else:
    print("✅ SUCCESS! All columns match exactly (treating Null and Blank as equal).")

mismatches.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Assuming 'spark' is an existing SparkSession
# If not, uncomment the following line:
# spark = SparkSession.builder.appName("DataComparison").getOrCreate()

# --- 1. SETUP ---
new_df = spark.table("LH_PROD_CP_CMR.VFADataValidation")
old_df = spark.table("LH_DEV_CP_CMR.pmbcp_vfadatavalidation_prod") # <--- REPLACE THIS

# --- 2. DEFINE THE MAPPING ---
column_mapping = {
    # -- Identifiers --
    "RAPID": "pmbcp_rapid",
    "REQID": "pmbcp_requirementid", # <--- UPDATED: Uses the readable ID column
    "AssetID": "pmbcp_assetid",
    
    # -- Core Data --
    "RequirementName": "pmbcp_requirementname",
    "RequirementDescription": "pmbcp_requirementdescription",
    "EstimatedCost": "pmbcp_estimatedcost",
    "Uniformat": "pmbcp_uniformat",
    "VFASystemGroup": "pmbcp_vfasystemgroup",
    "Category": "pmbcp_category",
    "Priority": "pmbcp_priority",
    "Impact": "pmbcp_impact",
    "IsRenewal": "pmbcp_isrenewal",
    "OutForAudit": "pmbcp_outforaudit",
    # "PlannedStatus": "pmbcp_plannedstatus", # <--- COMMENTED OUT: Caused AnalysisException (Column missing in old table)
    
    # -- Scores --
    "PriorityScore": "pmbcp_priorityscore", 
    "IoSFScore": "pmbcp_iosfscore",
    "PFAScore": "pmbcp_pfascore",
    "IoAFScore": "pmbcp_ioafscore",
    "ConditionScore": "pmbcp_conditionscore",
    "ReasonScore": "pmbcp_reasonscore",
    "ImpactScore": "pmbcp_impactscore",

    # -- Validation Checks --
    "EstCostCheck": "pmbcp_estimatedcostcheckvalue",
    #"EstCostErrorLevel": "pmbcp_estcosterrorlevel",
    "UniformatCheck": "pmbcp_uniformatcheckvalue",
    #"UniformatErrorLevel": "pmbcp_uniformaterrorlevel",
    "SystemGroupCheck": "pmbcp_systemgroupcheckvalue",
    #"SystemGroupErrorLevel": "pmbcp_systemgrouperrorlevel",
    "CategoryCheck": "pmbcp_requirementcategorycheckvalue",
    #"CategoryErrorLevel": "pmbcp_categoryerrorlevel",
    "PriorityCheck": "pmbcp_requirementprioritycheckvalue",
    #"PriorityErrorLevel": "pmbcp_priorityerrorlevel",
    "ImpactCheck": "pmbcp_requirementimpactcheckvalue",
    #"ImpactErrorLevel": "pmbcp_impacterrorlevel",
    "ReqNameCheck": "pmbcp_requirementnamecheckvalue",
    #"ReqNameErrorLevel": "pmbcp_reqnameerrorlevel",
    "ReqDescCheck": "pmbcp_requirementdescriptioncheckvalue",
    #"ReqDescErrorLevel": "pmbcp_reqdescerrorlevel",
    
    # -- Summary --
    "VFADataValidationTotalIssues": "pmbcp_totalissues",
    "VFADataValidationValidationMessage": "pmbcp_validationmessage"
}

# --- 3. PREPARE COMPARISON ---
# Join Keys
old_select_expr = [
    F.col("pmbcp_rapid").alias("RAPID"),
    F.col("pmbcp_requirementid").alias("REQID") # <--- UPDATED: Explicitly selecting the correct ID column
]

for new_col, old_col in column_mapping.items():
    if new_col not in ["RAPID", "REQID"]:
        old_select_expr.append(F.col(old_col).alias(f"{new_col}_old"))

old_subset = old_df.select(*old_select_expr)
comparison_df = new_df.join(old_subset, ["RAPID", "REQID"], "inner")

# --- 4. RUN COMPARISON ---
diff_filter = F.lit(False)
print("--- Checking for Mismatches (Ignoring Blank vs Null differences) ---")

# *** FIX APPLIED HERE: Initialize the list ***
failed_col_exprs = [] 

for col in column_mapping.keys():
    if col in ["RAPID", "REQID"]: continue
    
    new_val = F.col(col)
    old_val = F.col(f"{col}_old")
    
    # Base Transformation: Cast to string and Trim
    new_val_str = F.trim(new_val.cast("string"))
    old_val_str = F.trim(old_val.cast("string"))

    # SPECIAL HANDLING: IsRenewal (True/False vs Yes/No)
    if col == "IsRenewal":
        # 1. Normalize to lowercase
        new_val_str = F.lower(new_val_str)
        old_val_str = F.lower(old_val_str)
        
        # 2. Map "yes" -> "true" and "no" -> "false"
        # Note: PySpark casts Boolean True/False to "true"/"false" strings by default
        new_val_str = F.when(new_val_str == "yes", "true").when(new_val_str == "no", "false").otherwise(new_val_str)
        old_val_str = F.when(old_val_str == "yes", "true").when(old_val_str == "no", "false").otherwise(old_val_str)

    # SPECIAL HANDLING: EstimatedCost (Ignore Decimals: 100 vs 100.00)
    if col == "EstimatedCost":
        # Cast to double (handles "100.00"), Round to 0 decimals, Cast to Long (removes .0), then String
        new_val_str = F.round(new_val.cast("double"), 0).cast("long").cast("string")
        old_val_str = F.round(old_val.cast("double"), 0).cast("long").cast("string")

    # LOGIC: Coalesce to "" handles Nulls, allowing comparison with empty strings
    is_different = (
        F.coalesce(new_val_str, F.lit("")) != 
        F.coalesce(old_val_str, F.lit(""))
    )
    
    comparison_df = comparison_df.withColumn(f"Diff_{col}", is_different)
    diff_filter = diff_filter | is_different

    # If different, return the column name, otherwise null
    failed_col_exprs.append(F.when(is_different, F.lit(col)).otherwise(F.lit(None)))

# --- CREATE SUMMARY COLUMN ---
# Concatenate all failed column names into a comma-separated string
comparison_df = comparison_df.withColumn("Failed_Columns", F.concat_ws(", ", *failed_col_exprs))

# --- 5. REPORT RESULTS ---
mismatches = comparison_df.filter(diff_filter)
# CACHE: Critical for performance. Prevents re-running the heavy logic 30+ times in the loop below.
mismatches.cache() 

total_count = mismatches.count()

print(f"\nTotal Rows with Discrepancies: {total_count}")

# Safe display helper for non-notebook environments
def safe_display(df, limit=20):
    try:
        # Assumes 'display' is available in the environment (e.g., Databricks)
        display(df.limit(limit))
    except NameError:
        # Fallback for standard Python/Spark environments
        df.show(limit, truncate=False)

if total_count > 0:
    print("\nTop 20 Mismatches (Showing New vs Old):")
    
    display_cols = ["RAPID", "REQID", "Failed_Columns"] # Added Failed_Columns for better context
    for col in column_mapping.keys():
        if col in ["RAPID", "REQID"]: continue
        
        # We now add ALL columns to the display list to show full context.
        display_cols.extend([col, f"{col}_old"])
            
    safe_display(mismatches.select(*display_cols))
else:
    print("✅ SUCCESS! All columns match exactly (treating Null and Blank as equal).")

# Clean up cache
mismatches.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
