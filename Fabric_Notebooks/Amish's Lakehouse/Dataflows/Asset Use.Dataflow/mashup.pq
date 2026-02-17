[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Asset Use_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared Asset_Use = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Extension] = ".xlsx") and ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-PMB-CORP-PROJ13424/LakehouseUploads/Asset Use/")),
  #"Sorted rows" = Table.Sort(#"Filtered rows", {{"Date created", Order.Descending}}),
  #"Kept top rows" = Table.FirstN(#"Sorted rows", 1),
  Navigation = #"Kept top rows"{0}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{0}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Building ID", type text}, {"Building Name", type text}, {"Building Location Name (City)", type text}, {"Building Address", type text}, {"Building Ownership", type text}, {"Building Functional Status", type text}, {"Building Legal Status", type text}, {"Building Usable (m2)", type number}, {"Building Rentable (m2)", type number}, {"Building Gross (m2)", type number}, {"Building Type", type text}, {"Building Prime Function Category", type text}, {"Building Prime Function Type", type text}, {"Facility Manager Name", type text}, {"Site ID", type text}, {"Site Name", type text}}),
  #"Choose columns" = Table.SelectColumns(#"Changed column type", {"Building ID", "Building Type"})
in
  #"Choose columns";
shared #"Asset Use_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data],
  TableNavigation = Navigation_2{[Id = "Asset Use", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data]
in
  #"Navigation 2";
