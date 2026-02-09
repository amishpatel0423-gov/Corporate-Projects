[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "CMR Requirements 2026_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"CMR Requirements 2026" = let
  Source = Excel.Workbook(Web.Contents("https://abgov-my.sharepoint.com/personal/crystal_maceachern_gov_ab_ca/Documents/Apps/Microsoft Power Query/Uploaded Files/B2026 CMR Annual Project Planner_postSubmission.xlsx"), null, true),
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
  Navigation_2 = Navigation_1{[lakehouseId = "13ba4b10-e8a7-4302-8eea-4f35eb2aead9"]}[Data],
  TableNavigation = Navigation_2{[Id = "AnnualPlanner_B2026", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
