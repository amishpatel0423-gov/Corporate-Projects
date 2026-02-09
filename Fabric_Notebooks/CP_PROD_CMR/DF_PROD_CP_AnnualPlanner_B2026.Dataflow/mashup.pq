[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "CMR Requirements 2026_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"CMR Requirements 2026" = let
  Source = Excel.Workbook(Web.Contents("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR%20Template%20Source_Documentation/CMR%20Lakehouse/Release/DataFlow_Source/B2026%20CMR%20Annual%20Project%20Planner_postSubmission.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "CMR Requirements 2026", Kind = "Sheet"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type text}, {"Column10", type text}, {"Column11", type text}, {"Column12", type text}, {"Column13", type text}, {"Column14", type text}, {"Column15", type text}, {"Column16", type text}, {"Column17", type text}, {"Column19", type text}, {"Column20", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 1),
  #"Promoted headers" = Table.PromoteHeaders(#"Removed top rows", [PromoteAllScalars = true]),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Promoted headers", {{"Priority Score", Int64.Type}, {"Estimated Cost", Int64.Type}, {"Plan For Asset Score", Int64.Type}, {"Importance of Asset Functionality Score", Int64.Type}, {"Importance of System Functionality Score", Int64.Type}, {"Condition of System Score", Int64.Type}, {"Reason For Work Score", Int64.Type}, {"Impact of Failure Score", Int64.Type}, {"Requirement - Renewal", type logical}, {"Asset - Year Constructed", type date}, {"Asset Age", Int64.Type}}),
  #"Removed other columns" = Table.SelectColumns(#"Changed column type 1", {"Asset Number", "REQ ID"})
in
  #"Removed other columns";
shared #"CMR Requirements 2026_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a08f46d-4283-4451-a6c8-c231e572f46c"]}[Data],
  TableNavigation = Navigation_2{[Id = "PROD_AnnualPlanner_B2026", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
