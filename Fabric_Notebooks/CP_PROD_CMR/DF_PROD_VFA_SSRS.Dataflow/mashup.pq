[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "VFA_SSRS_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared VFA_SSRS = let
    //connect to the SSRS Report - uncomment the first filepath to test from local computer then comment out the sharepoint filepath
    //FilePath = "C:\_LocalData\VFA\VFA-LinkedRequirments_PROD.atomsvc",
    // 1. Define the direct URL, requesting CSV format
    ReportUri_CSV = "https://rpt.infrastructure.alberta.ca/ReportServer?%2FRAP-PM%2FVFA-LinkedRequirments&rs%3ACommand=Render&rs%3AFormat=csv&rc%3AItemPath=Tablix1",
    
    // 2. Retrieve the binary content (it's now CSV text)
    Request = Web.Contents(ReportUri_CSV),
    
    // 3. Parse the binary content as a CSV file
    // Set Delimiter to "," and ensure headers are handled.
    Data = Csv.Document(Request, [Delimiter = ",", Encoding = 1252, QuoteStyle = QuoteStyle.Csv]),
    
    // 4. Promote the first row to be column headers
    #"Promoted Headers" = Table.PromoteHeaders(Data, [PromoteAllScalars = true]),

    #"Changed Type Source" = Table.TransformColumnTypes(
        #"Promoted Headers",
        {
            {"Portfolio_Name", type text},
            {"Property_Name", type text},
            {"Asset_EID", type text},
            {"Asset_Name", type text},
            {"Building_ID", type text},
            {"Use_Type", type text},
            //{"Constructed_Year", Date.Type},
            {"Building_Priority_Detail", type text},
            {"Requirement_EID", type text},
            {"Requirement_Name", type text},
            {"Requirement_Description", type text},
            {"Linked_System", type any},
            {"Category", type text},
            {"Priority", type text},
            {"Impact", type text},
            {"EstimatedCost", Int64.Type},
            //{"RankingOne", Int64.Type},
            //{"RankingTwo", type text},
            {"RAPProjectID", type text},
            {"RAPProjectStatus", type text},
            {"System_EID", type text},
            {"System_Name", type text},
            {"Uniformat", type text},
            {"System_Group", type text},
            {"IsRenewal", type logical},
            {"OutForAudit", type logical}
        }
    ),
  #"Changed column type" = Table.TransformColumnTypes(#"Changed Type Source", {{"Linked_System", type text}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each Text.StartsWith([Portfolio_Name], "PR")),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Filtered rows", {{"Constructed_Year", Int64.Type}, {"RankingOne", Int64.Type}, {"RankingTwo", type text}}),
  #"Replace Win1252 Encoded Dash Portfolio" = Table.ReplaceValue(#"Changed column type 1","â€“","-",Replacer.ReplaceText,{"Portfolio_Name"}),
  #"Replace Win1252 Encoded Dash Sys Name" = Table.ReplaceValue(#"Replace Win1252 Encoded Dash Portfolio","â??","-",Replacer.ReplaceText,{"System_Name"}),
  #"Removed Blank Rows" = Table.SelectRows(#"Replace Win1252 Encoded Dash Sys Name", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
    //asset use is blank for sites, VFA SSRS manages trailers and parking lots already, this is different than when using a vfa data extract where three separate use columns are present. 
  #"Set Blank Site Use" = Table.ReplaceValue(#"Removed Blank Rows", "", "Site", Replacer.ReplaceValue, {"Use_Type"}),
  #"Rename to Data Model" = Table.RenameColumns(
        #"Set Blank Site Use",
        {
            {"Portfolio_Name", "PortfolioName"},
            {"Asset_Name", "AssetName"}, 
            {"Building_ID", "AssetID"},
            {"RAPProjectID", "RAPID"},
            {"Requirement_EID", "REQID"},
            {"RAPProjectStatus", "RAPStatus"},
            {"Use_Type", "AssetUse"},
            {"Building_Priority_Detail", "BuildingPriorityDetail"},
            {"EstimatedCost", "EstimatedCost"},
            {"Requirement_Description", "RequirementDescription"},
            {"Requirement_Name", "RequirementName"},
            {"System_Group", "VFASystemGroup"}, //distict the vfa system group from master system group in output of scoring model
            {"System_Name", "SystemName"}
        }
    ),
  //   #"Merged queries" = Table.NestedJoin(#"Rename to Data Model", {"RAPID"}, RAP_Project_Staging, {"RAPID"}, "RAP_Project_Staging", JoinKind.RightAnti),
    #"Join to Building List" = Table.NestedJoin(#"Rename to Data Model", {"AssetID"}, Building_List, {"AssetID"}, "Buiding_List", JoinKind.LeftOuter),
  #"Expanded Buiding_List" = Table.ExpandTableColumn(#"Join to Building List", "Buiding_List", {"Location"}, {"Location"}),
        //The Uniformat text in the SSRS doesn't have extra spaces between the uniformat code and the hyphen like the IoSF table does. (example: A1010 - Standard Foundations* on IoSF but A1010-Standard Foundations* in the report). Adds the extra spaces to the Uniformat column so the strings will match exactly. Using the full string to catch and check error condition messages for 150 year lifetimes have an * in dependent queries
    #"Uniformat Text Match Replace" = Table.TransformColumns(
        #"Expanded Buiding_List",
        {
            "Uniformat",
            each
                let
                    pos = Text.PositionOf(_, "-")
                    // Find first occurrence of "-"
                in
                    if pos <> -1 then
                        Text.Start(_, pos) & " - " & Text.Middle(_, pos + 1)
                    else
                        _,
            type text
        }
    ),
    //the VFA SSRS has empty string instead of null values. Replace all blanks (empty string) to null for condition check in dependent queries
        #"Replace Blank Report Values with Null" = Table.ReplaceValue(
        #"Uniformat Text Match Replace",
        "",
        null,
        Replacer.ReplaceValue,
        {"RequirementName", "RequirementDescription", "Category", "Priority", "Impact", "Uniformat", "VFASystemGroup" }
    ),
    //make sure requirement estimated cost is not null for any of the rows
    #"Replace Null Est Cost with 0" = Table.ReplaceValue(#"Replace Blank Report Values with Null",null,0,Replacer.ReplaceValue,{"EstimatedCost"}),
    #"Reordered Columns" = Table.ReorderColumns(#"Replace Null Est Cost with 0",{"RAPID", "RAPStatus", "PortfolioName", "AssetName", "AssetID", "AssetUse", "BuildingPriorityDetail", "REQID", "RequirementName", "RequirementDescription", "Category", "Priority", "Impact", "EstimatedCost", "SystemName", "Uniformat", "VFASystemGroup", "System_EID", "OutForAudit", "IsRenewal"
    , "Location"
    }
    ),
  #"Removed columns" = Table.RemoveColumns(#"Reordered Columns", {"Asset_EID", "Constructed_Year"}),
  #"Changed column type 2" = Table.TransformColumnTypes(#"Removed columns", {{"EstimatedCost", Int64.Type}})
in
    #"Changed column type 2";
shared Building_List = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "cr914_building_list", ItemKind = "Table"]}[Data],
  #"Filtered rows" = Table.SelectRows(#"Navigation 3", each ([cr914_owner] = "Infrastructure")),
  #"Removed other columns" = Table.SelectColumns(#"Filtered rows", {"cr914_bldgid", "cr914_location"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed other columns", {{"cr914_bldgid", "AssetID"}, {"cr914_location", "Location"}})
in
  #"Renamed columns";
shared VFA_SSRS_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a08f46d-4283-4451-a6c8-c231e572f46c"]}[Data],
  TableNavigation = Navigation_2{[Id = "PROD_VFA_SSRS", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
