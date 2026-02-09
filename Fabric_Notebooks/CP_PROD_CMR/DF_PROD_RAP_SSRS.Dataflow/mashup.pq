[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared ConfigurationTable = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "0a08f46d-4283-4451-a6c8-c231e572f46c"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "PROD_ConfigurationTable", ItemKind = "Table"]}[Data]
in
  #"Navigation 3";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "RAP_SSRS_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "RAPID", DestinationColumnName = "RAPID"], [SourceColumnName = "Name", DestinationColumnName = "Name"], [SourceColumnName = "RAPTitle", DestinationColumnName = "RAPTitle"], [SourceColumnName = "RAPStatus", DestinationColumnName = "RAPStatus"], [SourceColumnName = "RAPProjectDescription", DestinationColumnName = "RAPProjectDescription"], [SourceColumnName = "RAPProjectReason", DestinationColumnName = "RAPProjectReason"], [SourceColumnName = "RAPProjectConsequence", DestinationColumnName = "RAPProjectConsequence"], [SourceColumnName = "RAPProjectComments", DestinationColumnName = "RAPProjectComments"], [SourceColumnName = "Record_Created", DestinationColumnName = "Record_Created"], [SourceColumnName = "Deleted", DestinationColumnName = "Deleted"], [SourceColumnName = "Completed", DestinationColumnName = "Completed"], [SourceColumnName = "RAPTitleLengthCheck", DestinationColumnName = "RAPTitleLengthCheck"], [SourceColumnName = "RAPDescCheck", DestinationColumnName = "RAPDescCheck"], [SourceColumnName = "RAPDescLevel", DestinationColumnName = "RAPDescLevel"], [SourceColumnName = "RAPConseqCheck", DestinationColumnName = "RAPConseqCheck"], [SourceColumnName = "RAPConseqLevel", DestinationColumnName = "RAPConseqLevel"], [SourceColumnName = "RAPReasonCheck", DestinationColumnName = "RAPReasonCheck"], [SourceColumnName = "RAPDataValidation", DestinationColumnName = "RAPDataValidation"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared RAP_SSRS = let
    // 1. Define the direct URL, requesting CSV format
    ReportUri_CSV = "https://rpt.infrastructure.alberta.ca/ReportServer?%2FRAP-PM%2FRAP-Project%20Information%20Report&rs%3ACommand=Render&rs%3AFormat=csv&rc%3AItemPath=Tablix1",
    
    // 2. Retrieve the binary content (it's now CSV text)
    Request = Web.Contents(ReportUri_CSV),
    
    // 3. Parse the binary content as a CSV file
    Data = Csv.Document(Request, [Delimiter = ",", Encoding = 1252, QuoteStyle = QuoteStyle.Csv]),
    
    // 4. Promote the first row to be column headers
    #"Promoted Headers" = Table.PromoteHeaders(Data, [PromoteAllScalars = true]),
  #"Removed columns" = Table.RemoveColumns(#"Promoted Headers", {"raApprovedTPC", "raProjMgr", "Initiator", "FUDesc"}),
    #"Rename to dataModel" = Table.RenameColumns(
        #"Removed columns",
        {
            //{"Name", "Asset Name"},
            {"Title", "RAPTitle"},
            {"Status", "RAPStatus"},
            //{"raProjMgr", "RAP Project Manager"},
            //{"FUDesc", "Funding Description"},
            {"Description", "RAPProjectDescription"},
            {"Reason", "RAPProjectReason"},
            {"Consequences", "RAPProjectConsequence"},
            {"Critical", "RAPProjectComments"}
        }
    ),
    
    // ** Combine Property and Number into RAPID **
    #"Combine for RAP ID" = Table.CombineColumns(
        #"Rename to dataModel", {"Property", "Number"}, Combiner.CombineTextByDelimiter("-", QuoteStyle.None), "RAPID"
    ),

    // 5. Explicitly set the type *before* the record transformation
    #"Make Record_Create DateTime" = Table.TransformColumnTypes(#"Combine for RAP ID", {{"Record_Created", type datetime}}),
    
    // Define the list of combined RAPIDs to update
    // NOTE: This list now contains the full RAPID string, which is correct.
    BringRAPForwardToUpdate = {
        "S0395-006",
        "S0001-253",
        "B0731A-045",
        "B0531A-034"
    },
    
    UpdatedTable = Table.FromRecords(
        List.Transform(
            Table.ToRecords(#"Make Record_Create DateTime"),
            each if List.Contains(BringRAPForwardToUpdate, {_[RAPID]}) then // Check the combined [RAPID] column
                // Replacement value must be #datetime to match the column type
                Record.TransformFields(_, {{"Record_Created", each #datetime(2001, 1, 1, 0, 0, 0)}})
            else _
        )
    ),
    

    #"Replace Blanks with Null" = Table.ReplaceValue(
        UpdatedTable,
        "",
        null,
        Replacer.ReplaceValue,
        {"RAPProjectDescription", "RAPProjectReason", "RAPProjectConsequence", "RAPProjectComments"}
    ),
    
    #"Filter by RAP Status" = Table.SelectRows(#"Replace Blanks with Null", each ([RAPStatus] <> "" and [RAPStatus] <> "DE")),
    #"Filter by Deleted" = Table.SelectRows(#"Filter by RAP Status", each ([Deleted] = "No")),
    #"Trim RAP ID" = Table.TransformColumns(#"Filter by Deleted", {{"RAPID", Text.Trim, type text}}),

    // Simplified and corrected date filtering logic
    RAPFilterConfigDateRow = Table.SelectRows(ConfigurationTable, each [ConfigurationName] = "RAPFilterDate"),
    // Get the date value or default to 5 years ago
    RAPFilterDateValue = if Table.RowCount(RAPFilterConfigDateRow) > 0 
                         then Value.Type(RAPFilterConfigDateRow{0}[Value]) 
                         else Date.AddYears(DateTime.LocalNow(), -5),
                         
    // Ensure the retrieved value is a proper DateTime before comparing
    RAPFilterDate = if Value.Is(RAPFilterDateValue, type date) or Value.Is(RAPFilterDateValue, type datetime) then 
                        RAPFilterDateValue 
                    else 
                        Date.AddYears(DateTime.LocalNow(), -5),
    
    // Apply the final date filter
    #"Filtered by Config Date" = Table.SelectRows(#"Trim RAP ID", each [Record_Created] >= RAPFilterDate),

    // Final output is the filtered table
    Result = #"Filtered by Config Date",
    #"Sort by Create Date" = Table.Sort(Result, {{"Record_Created", Order.Descending}}),
    #"Check RAP Title Length" = Table.AddColumn(
        #"Sort by Create Date",
        "RAPTitleLengthCheck",
        each
            if Text.Length([#"RAPTitle"]) > 40 then
                fnGetParameters("msg_warnRAPTitleLength")[MsgTxt]
            else
                "--"
    ),
    #"Check RAP Description" = Table.AddColumn(
        #"Check RAP Title Length",
        "RAPDescCheck",
        each if ([RAPProjectDescription] = null) or ([RAPProjectDescription] = "") then fnGetParameters("msg_errorRAPDesc")[MsgTxt] 
        else fnGetParameters("msg_passRAPDescValidation")[MsgTxt]
    ),
    #"Get RAP Description Error Level" = Table.AddColumn(
        #"Check RAP Description",
        "RAPDescLevel",
        each if ([RAPDescCheck] <> "--") 
            then fnGetParameters("msg_errorRAPDesc")[MessageLevel] 
            else null
    ),

    #"Check RAP Consequence" = Table.AddColumn(
        #"Get RAP Description Error Level",
        "RAPConseqCheck",
        each if ([RAPProjectConsequence] = null) or ([RAPProjectConsequence] = "") then fnGetParameters("msg_errorRAPConsequence")[MsgTxt] 
        else fnGetParameters("msg_passRAPConsValidation")[MsgTxt]
    ),
    #"Get RAP Consequence Error Level" = Table.AddColumn(
    #"Check RAP Consequence",
    "RAPConseqLevel",
    each if ([RAPConseqCheck] <> "--") 
        then fnGetParameters("msg_errorRAPDesc")[MessageLevel] 
        else null
),
    #"Check RAP Reason" = Table.AddColumn(
        #"Get RAP Consequence Error Level",
        "RAPReasonCheck",
        each if ([RAPProjectReason] = null) or ([RAPProjectReason] = "") then fnGetParameters("msg_errorRAPReason")[MsgTxt]  
        else fnGetParameters("msg_passRAPReasonValidation")[MsgTxt] 
    ),
    #"Get RAP Reason Error Level" = Table.AddColumn(
    #"Check RAP Reason",
    "RAPReasonLevel",
    each if ([RAPReasonCheck] <> "--") 
        then fnGetParameters("msg_errorRAPDesc")[MessageLevel] 
        else null
),
#"check function" = fnGetParameters("msg_errorRAPDataValidation"),
#"Data Validation Flag" = Table.AddColumn(#"Check RAP Reason", "RAPDataValidation",  
    each 
    let
        columns = {
            Record.FieldOrDefault(_, "RAPDescLevel", null),
            Record.FieldOrDefault(_, "RAPConseqLevel", null),
            Record.FieldOrDefault(_, "RAPReasonLevel", null)
        },
        errorCount = List.Count(
            List.Select(columns, each _ = 1)
        ),
        validationText = 
            if errorCount > 0 then
               Text.From(errorCount) //fnGetParameters("msg_errorRAPDataValidation")[MsgTxt] & " " & Text.From(errorCount)
            else
                0
    in
        validationText
),
  #"Changed column type" = Table.TransformColumnTypes(#"Data Validation Flag", {{"RAPID", type text}, {"Name", type text}, {"RAPTitle", type text}, {"RAPStatus", type text}, {"RAPProjectDescription", type text}, {"RAPProjectReason", type text}, {"RAPProjectConsequence", type text}, {"RAPProjectComments", type text}, {"Record_Created", type text}, {"Deleted", type text}, {"Completed", type text}, {"RAPTitleLengthCheck", type text}, {"RAPDescCheck", type text}, {"RAPDescLevel", type text}, {"RAPConseqCheck", type text}, {"RAPConseqLevel", type text}, {"RAPReasonCheck", type text}, {"RAPDataValidation", type text}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Changed column type", {{"RAPDescLevel", Int64.Type}, {"RAPConseqLevel", Int64.Type}, {"RAPDataValidation", Int64.Type}, {"Record_Created", type datetime}})
in 
    #"Changed column type 1";
shared fnGetParameters = (ConfigName as text) as any =>
let
    ConfigSource = ConfigurationTable,
    AddLowerCase = Table.AddColumn(ConfigSource, "ConfigNameLower", each Text.Lower([ConfigurationName]), type text),
    ConfigRow = Table.SelectRows(AddLowerCase, each [ConfigNameLower] = Text.Lower(ConfigName)),
    HasRow = not Table.IsEmpty(ConfigRow),
    MsgTxt = if HasRow then ConfigRow{0}[Value] else null,
    MsgLevel = if HasRow and Record.HasFields(ConfigRow{0}, "MessageLevel") then ConfigRow{0}[MessageLevel] else null,
    Output = if MsgLevel = null or MsgLevel = 0 then MsgTxt else [MsgTxt = MsgTxt, MessageLevel = MsgLevel]
in
    Output;
shared RAP_SSRS_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a08f46d-4283-4451-a6c8-c231e572f46c"]}[Data],
  TableNavigation = Navigation_2{[Id = "PROD_RAP_SSRS", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
