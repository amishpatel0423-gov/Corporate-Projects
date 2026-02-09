[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "PROD_DV_Building_List_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared PROD_DV_Building_List = let
  Source = CommonDataService.Database("org509f7c88.crm3.dynamics.com", [CreateNavigationProperties = null]),
  #"Navigation 1" = Source{[Schema = "dbo", Item = "cr914_building_list"]}[Data],
  #"Removed other columns" = Table.SelectColumns(#"Navigation 1", {"cr914_bldgid", "cr914_location", "cr914_owner"}),
  #"Filtered rows" = Table.SelectRows(#"Removed other columns", each ([cr914_owner] = "Infrastructure")),
  #"Renamed columns" = Table.RenameColumns(#"Filtered rows", {{"cr914_bldgid", "AssetID"}, {"cr914_location", "Location"}, {"cr914_owner", "Owner"}})
in
  #"Renamed columns";
shared PROD_DV_Building_List_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a08f46d-4283-4451-a6c8-c231e572f46c"]}[Data],
  TableNavigation = Navigation_2{[Id = "PROD_DV_Building_List", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
