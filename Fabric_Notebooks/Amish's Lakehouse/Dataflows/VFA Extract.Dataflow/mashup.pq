[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "VFAExtract_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Asset - Portfolio Name", DestinationColumnName = "Asset - Portfolio Name"], [SourceColumnName = "Asset - Asset Name", DestinationColumnName = "Asset - Asset Name"], [SourceColumnName = "Asset - Number", DestinationColumnName = "Asset - Number"], [SourceColumnName = "Asset - Building Priority Detail", DestinationColumnName = "Asset - Building Priority Detail"], [SourceColumnName = "Asset - Use", DestinationColumnName = "Asset - Use"], [SourceColumnName = "Asset - FCI", DestinationColumnName = "Asset - FCI"], [SourceColumnName = "Requirement - Linked System", DestinationColumnName = "Requirement - Linked System"], [SourceColumnName = "Requirement - Category", DestinationColumnName = "Requirement - Category"], [SourceColumnName = "Requirement - Requirement Name", DestinationColumnName = "Requirement - Requirement Name"], [SourceColumnName = "Requirement - Estimated Cost", DestinationColumnName = "Requirement - Estimated Cost"], [SourceColumnName = "Requirement - Priority", DestinationColumnName = "Requirement - Priority"], [SourceColumnName = "Requirement - Impact", DestinationColumnName = "Requirement - Impact"], [SourceColumnName = "Requirement - RAP Project ID", DestinationColumnName = "Requirement - RAP Project ID"], [SourceColumnName = "Requirement - ID", DestinationColumnName = "Requirement - ID"], [SourceColumnName = "Asset - City", DestinationColumnName = "Asset - City"], [SourceColumnName = "Requirement - System Group", DestinationColumnName = "Requirement - System Group"], [SourceColumnName = "Requirement - RAP Project Status", DestinationColumnName = "Requirement - RAP Project Status"], [SourceColumnName = "Requirement - Ranking #2", DestinationColumnName = "Requirement - Ranking #2"], [SourceColumnName = "Requirement - Renewal", DestinationColumnName = "Requirement - Renewal"], [SourceColumnName = "Requirement - Action FY", DestinationColumnName = "Requirement - Action FY"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared VFAExtract = let
  Source = SharePoint.Files("https://abgov.sharepoint.com/sites/S300D13-ICMR", [ApiVersion = 15]),
  #"Filtered rows" = Table.SelectRows(Source, each ([Folder Path] = "https://abgov.sharepoint.com/sites/S300D13-ICMR/CMR System Admin/VFA Extracts/")),
  #"Sorted rows 1" = Table.Sort(#"Filtered rows", {{"Date created", Order.Descending}}),
  #"Kept top rows 1" = Table.FirstN(#"Sorted rows 1", 1),
  Navigation = #"Kept top rows 1"{0}[Content],
  #"Imported Excel workbook" = Excel.Workbook(Navigation, null, true),
  #"Navigation 1" = #"Imported Excel workbook"{0}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"Asset - Portfolio Name", type text}, {"Asset - Asset Name", type text}, {"Asset - Number", type text}, {"Asset - Building Priority Detail", type text}, {"Asset - Use", type text}, {"Asset - FCI", type number}, {"Requirement - Linked System", type text}, {"Requirement - Category", type text}, {"Requirement - Requirement Name", type text}, {"Requirement - Estimated Cost", Int64.Type}, {"Requirement - Priority", type text}, {"Requirement - Impact", type text}, {"Requirement - RAP Project ID", type text}, {"Requirement - ID", type text}, {"Asset - City", type text}, {"Requirement - System Group", type text}, {"Requirement - RAP Project Status", type text}, {"Requirement - Ranking #2", type text}, {"Requirement - Renewal", type logical}, {"Requirement - Action FY", Int64.Type}})
in
  #"Changed column type";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data]
in
  #"Navigation 2";
shared VFAExtract_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d6dd6348-2d7c-4a9f-a109-2eb833ebc56e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "ce627731-9b73-4ac7-8826-348580796c17"]}[Data],
  TableNavigation = Navigation_2{[Id = "VFAExtract", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
