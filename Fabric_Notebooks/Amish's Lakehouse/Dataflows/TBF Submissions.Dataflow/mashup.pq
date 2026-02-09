[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "B2023_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared B2023 = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/Budget Prep/Previous Years TBF Submissions/")),
  Navigation = #"Filtered rows"{[Name = "2023 CMR Annual Project Planner.xlsx", #"Folder Path" = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/Budget Prep/Previous Years TBF Submissions/"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{[Item = "Working", Kind = "Sheet"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}, {"Column16", type text}, {"Column17", type text}, {"Column18", type text}, {"Column19", type text}, {"Column20", type text}, {"Column21", type text}, {"Column22", type text}, {"Column23", type text}, {"Column24", type text}, {"Column25", type text}, {"Column26", type text}, {"Column27", type text}, {"Column28", type text}, {"Column29", type text}, {"Column30", type text}, {"Column31", type text}, {"Column32", type text}, {"Column33", type text}, {"Column34", type text}, {"Column36", type text}, {"Column38", type text}, {"Column40", type text}, {"Column42", type text}, {"Column44", type text}}),
  #"Promoted headers" = Table.PromoteHeaders(#"Changed column type", [PromoteAllScalars = true]),
  #"Promoted headers 1" = Table.PromoteHeaders(#"Promoted headers", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers 1", {{"FCI", type number}, {"Est Cost", Int64.Type}, {"PFA_Rating", Int64.Type}, {"IoAF_Rating", Int64.Type}, {"IoSF_Rating", Int64.Type}, {"Condition_Rating", Int64.Type}, {"Reason_Rating", Int64.Type}, {"Failure_Rating", Int64.Type}, {"Priority Score", Int64.Type}})
in
  #"Changed column type 1";
shared B2023_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data],
  TableNavigation = Navigation_2{[Id = "B2023", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "B2024_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "Req ID", DestinationColumnName = "Requirement - ID"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared B2024 = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/Budget Prep/Previous Years TBF Submissions/")),
  Navigation = #"Filtered rows"{[Name = "2024 CMR Annual Project Planner.xlsx", #"Folder Path" = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/Budget Prep/Previous Years TBF Submissions/"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{[Item = "CMR Working", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"S/BID", type text}, {"Asset Name", type text}, {"Location", type text}, {"Req ID", type text}, {"Priority Score", Int64.Type}, {"Req Name", type text}, {"Req Description", type text}, {"Req System Group", type text}, {"System Group", type text}, {"Req Est Cost", Int64.Type}, {"Bldg Priority Detail", type text}, {"Plan for Asset Rating", type text}, {"PFA Score", Int64.Type}, {"Asset Use", type text}, {"Importance of Asset Functionality", type text}, {"IoAF Score", Int64.Type}, {"Req Linked System", type text}, {"Importance of System Functionality", type text}, {"IoSF Score", Int64.Type}, {"Req Priority", type text}, {"Condition Rating", type text}, {"Condition Score", Int64.Type}, {"Req Category", type text}, {"Reason Rating", type text}, {"CMR Status", type text}, {"Reason Score", Int64.Type}, {"Req Impact", type text}, {"Impact Rating", type text}, {"Impact Score", Int64.Type}, {"CMR Submission", type text}, {"RAP ID", type text}, {"RAP Status", type text}, {"Director Name", type text}, {"FM Name", type text}, {"Portfolio Code", type text}, {"System Uniformat Code", type text}, {"System Uniformat", type text}, {"Past Score", Int64.Type}, {"Score Change", type text}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "B2025_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared B2025 = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/Budget Prep/Previous Years TBF Submissions/")),
  Navigation = #"Filtered rows"{[Name = "2025 CMR Annual Project Planner.xlsx", #"Folder Path" = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/Budget Prep/Previous Years TBF Submissions/"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{[Item = "CMRRequirements2025", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Asset Number", type text}, {"Asset Name", type text}, {"Priority Score", Int64.Type}, {"REQ ID", type text}, {"Estimated Cost", Int64.Type}, {"Requirement Name", type text}, {"Requirement Description", type text}, {"System Group", type text}, {"Linked System", type text}, {"Building Priority Detail", type text}, {"Plan for Asset", type text}, {"PFA.Score", Int64.Type}, {"Asset Use", type text}, {"Importance of Asset Functionality", type text}, {"IoAF.Score", Int64.Type}, {"Importance of System Functionality", type text}, {"IoSF.Score", Int64.Type}, {"Priority", type text}, {"Condition of System", type text}, {"Condition.Score", Int64.Type}, {"Category", type text}, {"Reason for Work", type text}, {"Reason.Score", Int64.Type}, {"Impact", type text}, {"Impact of Failure", type text}, {"Impact.Score", Int64.Type}, {"RAP ID", type text}, {"RAP Status", type text}, {"Asset - Year Constructed", type date}, {"Requirement - Renewal", type logical}, {"Location", type text}, {"Director Name", type text}, {"Facility Manager Name", type text}, {"Asset Age", Int64.Type}, {"Uniformat Code", type text}})
in
  #"Changed column type";
shared B2025_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data],
  TableNavigation = Navigation_2{[Id = "B2025", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data]
in
  #"Navigation 2";
[BindToDefaultDestination = true]
shared B2026 = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Extension] = ".xlsx")),
  Navigation = #"Filtered rows"{[Name = "B2026 CMR Annual Project Planner.xlsx", #"Folder Path" = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/Budget Prep/B2026/"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{[Item = "CMRRequirements2026", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Priority Score", Int64.Type}, {"Asset Name", type text}, {"Asset Number", type text}, {"Location", type text}, {"Director Name", type text}, {"Facility Manager Name", type text}, {"Requirement Name", type text}, {"Requirement Description", type text}, {"REQ ID", type text}, {"RAP ID", type text}, {"RAP Status", type text}, {"Building Priority Detail", type text}, {"Asset Use", type text}, {"Uniformat", type text}, {"Priority", type text}, {"Category", type text}, {"Impact", type text}, {"Estimated Cost", Int64.Type}, {"System Group", type text}, {"Linked System", type text}, {"Plan For Asset Score", Int64.Type}, {"Importance of Asset Functionality Score", Int64.Type}, {"Importance of System Functionality Score", Int64.Type}, {"Condition of System Score", Int64.Type}, {"Reason For Work Score", Int64.Type}, {"Impact of Failure Score", Int64.Type}, {"Requirement - Renewal", type logical}, {"Asset - Year Constructed", type date}, {"Asset Age", Int64.Type}})
in
  #"Changed column type";
shared B2024_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data],
  TableNavigation = Navigation_2{[Id = "B2024", ItemKind = "Table"]}[Data]
in
  TableNavigation;
