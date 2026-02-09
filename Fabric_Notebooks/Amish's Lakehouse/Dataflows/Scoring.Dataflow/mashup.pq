[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared IoSF = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/")),
  Navigation = #"Filtered rows"{[Name = "Scoring (Used for Lakehouse).xlsx", #"Folder Path" = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{[Item = "IoSF", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"System - Uniformat", type text}, {"System Group", type text}, {"System - Uniformat Code", type text}, {"Importance of System Functionality", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared IoAF = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/")),
  Navigation = #"Filtered rows"{[Name = "Scoring (Used for Lakehouse).xlsx", #"Folder Path" = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{[Item = "IoAF", Kind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 1", {{"Asset Use", type text}, {"Importance of Asset Functionality", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared #"Plan for Asset" = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/")),
  Navigation = #"Filtered rows"{[Name = "Scoring (Used for Lakehouse).xlsx", #"Folder Path" = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{[Item = "PlanforAsset", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Building Priority Detail", type text}, {"Plan for Asset", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared Requirement_Category = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/")),
  Navigation = #"Filtered rows"{[Name = "Scoring (Used for Lakehouse).xlsx", #"Folder Path" = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{[Item = "Requirement Category", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Requirement Category", type text}, {"Reason for Work", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared #"Requirement Priority" = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/")),
  Navigation = #"Filtered rows"{[Name = "Scoring (Used for Lakehouse).xlsx", #"Folder Path" = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{[Item = "Requirement Priority", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Requirement Priority", type text}, {"Condition of System", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared #"Requirement Impact" = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/")),
  Navigation = #"Filtered rows"{[Name = "Scoring (Used for Lakehouse).xlsx", #"Folder Path" = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/InfrasCMR/CMR Process Improvement/"]}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{[Item = "Requirement Impact", Kind = "Sheet"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Requirement Impact", type text}, {"Impact of Failure", type text}, {"Score", Int64.Type}, {"Score with Weight", Int64.Type}, {"Weight", Int64.Type}})
in
  #"Changed column type";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data]
in
  #"Navigation 2";
