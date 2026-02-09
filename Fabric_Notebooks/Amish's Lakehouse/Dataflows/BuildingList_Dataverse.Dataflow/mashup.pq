[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data]{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data];
[BindToDefaultDestination = true]
shared cr914_building_list = let
  Source = CommonDataService.Database("orgf4e4efec.crm3.dynamics.com", [CreateNavigationProperties = null]),
  #"Navigation 1" = Source{[Schema = "dbo", Item = "cr914_building_list"]}[Data]
in
  #"Navigation 1";
