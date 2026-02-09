[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "PlannedStatusModel_dev1_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared PlannedStatusModel_dev1 = let
  Source = Excel.Workbook(Web.Contents("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR%20Template%20Source_Documentation/CMR%20Lakehouse/Release/DataFlow_Source/CMR%20Annual%20Project%20Planner%20B2025.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "PlannedStatusModel_dev", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"CMR Number", Int64.Type}, {"Project Priority Score", Int64.Type}, {"Asset Number", type text}, {"Asset Name", type text}, {"REQ ID", type text}, {"System Group", type text}, {"Linked System", type text}}),
  #"Removed other columns" = Table.SelectColumns(#"Changed column type", {"Asset Number", "REQ ID"})
in
  #"Removed other columns";
shared PlannedStatusModel_dev1_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a08f46d-4283-4451-a6c8-c231e572f46c"]}[Data],
  TableNavigation = Navigation_2{[Id = "PROD_AnnualPlanner_B2025", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
