[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data]{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data];
[BindToDefaultDestination = true]
shared pmbcp_cmrprojreqmain = let
  Source = CommonDataService.Database("org509f7c88.crm3.dynamics.com", [CreateNavigationProperties = null]),
  #"Navigation 1" = Source{[Schema = "dbo", Item = "pmbcp_cmrprojreqmain"]}[Data]
in
  #"Navigation 1";
