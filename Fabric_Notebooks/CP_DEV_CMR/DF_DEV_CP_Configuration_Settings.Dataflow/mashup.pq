[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "ConfigurationTable_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "ConfigurationName", DestinationColumnName = "ConfigurationName"], [SourceColumnName = "Value", DestinationColumnName = "Value"], [SourceColumnName = "MessageLevel", DestinationColumnName = "MessageLevel"], [SourceColumnName = "MessageCategory", DestinationColumnName = "MessageCategory"], [SourceColumnName = "Description", DestinationColumnName = "Description"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared ConfigurationTable = let
  Source = Excel.Workbook(Web.Contents("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR%20Template%20Source_Documentation/CMR%20Lakehouse/Release/DataFlow_Source/ConfigurationSettings.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "ConfigurationTable", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"ConfigurationName", type text}, {"MessageLevel", Int64.Type}, {"MessageCategory", type text}, {"Description", type text}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 5),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Removed top rows", {{"Value", type text}})
in
  #"Changed column type 1";
shared ConfigurationTable_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "13ba4b10-e8a7-4302-8eea-4f35eb2aead9"]}[Data],
  TableNavigation = Navigation_2{[Id = "ConfigurationTable", ItemKind = "Table"]}[Data]
in
  TableNavigation;
