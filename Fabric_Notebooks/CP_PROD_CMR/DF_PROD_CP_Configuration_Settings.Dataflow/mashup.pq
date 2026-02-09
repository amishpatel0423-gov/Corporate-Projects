[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "PROD_ConfigurationTable_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared PROD_ConfigurationTable = let
  Source = Excel.Workbook(Web.Contents("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR%20Template%20Source_Documentation/CMR%20Lakehouse/Release/DataFlow_Source/ConfigurationSettings.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "ConfigurationTable", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"ConfigurationName", type text}, {"MessageLevel", Int64.Type}, {"MessageCategory", type text}, {"Description", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 5),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Value", type text}})
in
  #"Changed column type 1";
shared PROD_ConfigurationTable_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0a08f46d-4283-4451-a6c8-c231e572f46c"]}[Data],
  TableNavigation = Navigation_2{[Id = "PROD_ConfigurationTable", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
