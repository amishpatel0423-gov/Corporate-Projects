[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "PlannedStatusModel_dev1_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared PlannedStatusModel_dev1 = let
  Source = Excel.Workbook(Web.Contents("https://abgov-my.sharepoint.com/personal/crystal_maceachern_gov_ab_ca/Documents/Apps/Microsoft Power Query/Uploaded Files/CMR Annual Project Planner B2025.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "PlannedStatusModel_dev", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"CMR Number", Int64.Type}, {"Project Priority Score", Int64.Type}, {"Asset Number", type text}, {"Asset Name", type text}, {"REQ ID", type text}, {"System Group", type text}, {"Linked System", type text}}),
  #"Removed other columns" = Table.SelectColumns(#"Changed column type", {"Asset Number", "REQ ID"})
in
  #"Removed other columns";
shared PlannedStatusModel_dev1_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "13ba4b10-e8a7-4302-8eea-4f35eb2aead9"]}[Data],
  TableNavigation = Navigation_2{[Id = "AnnualPlanner_B2025", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
