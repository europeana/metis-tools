**Migration tool from SugarCRM exported CSV file to Metis database**  
This project contains functionality and documentation of how an export from SugarCRM that contains
dataset information for Europeana can be parsed and mapped to the new Metis database.  


**SugarCRM to Metis database field mappings**  
CSV Column -> Dataset Field  
NM = Not Mapped  
  
NM, Auto generated to NOT_CREATED_YET-UUID -> ecloudDatasetId  
("Name" Column 0)Extracted from the first part of the string until "_" only numeric values -> datasetId  
("Name" Column 0)Mapped column 0 -> datasetName  
NM, Harcoded from Zoho -> organizationId  
NM, Harcoded from Zoho -> organizationName  
NM -> provider  
NM -> intermediateProvider  
NM -> dataProvider  
NM, Harcoded from Zoho -> createdByUserId  
("Date Created" Column 19) -> createdDate  
NM -> updatedDate  
NM Some info on Notes column 64 "Notes", Not possible to be retrieved -> replacedBy  
NM Some info on Notes column 64 "Notes", Not possible to be retrieved -> replaces  
("Dataset Country" Column 68)(There are also similar columns 44 and 66) -> country  
NM ?Language information not available -> language  
("Descritpion" Column 10) -> description    
("Notes" Column 64) -> notes

If "Harvesting Type" Column 75 is "oai_pmh" The following will also be mapped.  
("Harvest URL" Column 43) -> OaipmhHarvestPluginMetadata.url
("Metadata Format" Column 26) -> OaipmhHarvestPluginMetadata.metadataFormat
("Setspec" Column 58) If "-" then it will not be mapped -> OaipmhHarvestPluginMetadata.setSpec
NM -> OaipmhHarvestPluginMetadata.fromDate
NM -> OaipmhHarvestPluginMetadata.untilDate

If "Harvesting Type" Column 75 is not "oai_pmh", then no harvesting information will be added
since at the time of writing http metadata are not ready so these will be bypassed. 

