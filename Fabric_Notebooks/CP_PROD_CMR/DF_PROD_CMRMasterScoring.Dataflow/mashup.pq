[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared PROD_IoAF = let
  Source = Excel.Workbook(Web.Contents("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR%20Template%20Source_Documentation/CMR%20Source%20Code%20Repo/CommonModules/CommonMasterScoring/Master_CMR_ScoringTables.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "IoAF", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Asset Use", type text}, {"Importance of Asset Functionality", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared PROD_IoSF = let
  Source = Excel.Workbook(Web.Contents("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR%20Template%20Source_Documentation/CMR%20Source%20Code%20Repo/CommonModules/CommonMasterScoring/Master_CMR_ScoringTables.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "IoSF", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"System - Uniformat", type text}, {"System Group", type text}, {"System - Uniformat Code", type text}, {"Importance of System Functionality", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared PROD_PlanForAsset = let
  Source = Excel.Workbook(Web.Contents("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR%20Template%20Source_Documentation/CMR%20Source%20Code%20Repo/CommonModules/CommonMasterScoring/Master_CMR_ScoringTables.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "PlanForAsset", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Building Priority Detail", type text}, {"Plan for Asset", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared PROD_ImpactOfFailure = let
  Source = Excel.Workbook(Web.Contents("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR%20Template%20Source_Documentation/CMR%20Source%20Code%20Repo/CommonModules/CommonMasterScoring/Master_CMR_ScoringTables.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "ImpactOfFailure", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Requirement Impact", type text}, {"Impact of Failure", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared PROD_ReasonForWork = let
  Source = Excel.Workbook(Web.Contents("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR%20Template%20Source_Documentation/CMR%20Source%20Code%20Repo/CommonModules/CommonMasterScoring/Master_CMR_ScoringTables.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "ReasonForWork", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Requirement Category", type text}, {"Reason for Work", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared PROD_ConditionOfSystem = let
  Source = Excel.Workbook(Web.Contents("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR%20Template%20Source_Documentation/CMR%20Source%20Code%20Repo/CommonModules/CommonMasterScoring/Master_CMR_ScoringTables.xlsx"), null, true),
  #"Navigation 1" = Source{[Item = "ConditionOfSystem", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Requirement Priority", type text}, {"Condition of System", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "0a08f46d-4283-4451-a6c8-c231e572f46c"]}[Data]
in
  #"Navigation 2";
