**Migration tool from SugarCRM exported CSV file to Metis database**  
This project contains functionality and documentation of how an export from SugarCRM that contains
dataset information for Europeana can be parsed and mapped to the new Metis database.  


**SugarCRM to Metis database field mappings**  
CSV Column -> Dataset Field  
NM = Not Mapped  
  
NM, Auto generated to NOT_CREATED_YET-UUID -> ecloudDatasetId  
("Name" Column 0)Extracted from the first part of the string until "_" -> datasetId  
("Name" Column 0)Extracted from the last part of the string after the first "_" character -> datasetName  
NM, Harcoded from Zoho -> organizationId  
NM, Harcoded from Zoho -> organizationName  
("Organisation Name" Column 9) -> provider  
NM -> intermediateProvider  
NM -> dataProvider  
NM, Harcoded from Zoho -> createdByUserId  
("Date Created" Column 19) -> createdDate  
NM -> updatedDate  
NM Some info on Notes column 64 "Notes", Not possible to be retrieved -> replacedBy  
NM Some info on Notes column 64 "Notes", Not possible to be retrieved -> replaces  
("Dataset Country" Column 68) -> country  
NM ?Language information not available -> language  
("Descritpion" Column 10) -> description    
("Notes" Column 64) -> notes

If "Harvesting Type" Column 75 is "oai_pmh" The following will also be mapped.  
("Harvest URL" Column 43) -> OaipmhHarvestPluginMetadata.url (Checks if url is valid otherwise set to null)
("Metadata Format" Column 26) -> OaipmhHarvestPluginMetadata.metadataFormat
("Setspec" Column 58) If "-" then it will not be mapped -> OaipmhHarvestPluginMetadata.setSpec
NM -> OaipmhHarvestPluginMetadata.fromDate
NM -> OaipmhHarvestPluginMetadata.untilDate

If "Harvesting Type" Column 75 is not "oai_pmh" then only one field will be stored if it's a valid url  
("HTTP URL" Column 49) -> HTTPHarvestPluginMetadata.url

**Updating configuration**  
A migration.properties file should be created under src/main/resources and the relative parameters should be populated.
Most of the fields are self explanatory and the ones that require some attention are:
- `organization.id` -> Is the organizationId that will be used for all the datasets, it should be a real id from Zoho
- `organization.name` -> Is the organizationName corresponding to the organizationId that will be used for all the datasets, it should be a real name from Zoho
- `user.id` -> Is the userId that will be used for all the datasets, it should be a real id from Zoho
- `mode` -> Can be either CREATE for reading a csv and creating Datasets, or DELETE to delete datasetIds from the database using a dataset ids file
- `dataset.ids.path` -> Is used if mode is DELETE and contains an identifier per line
- `truststore.path/truststore.password` -> are used if the database to connect has self signed certificates and this trustore should contain that certificate

**Running the script**
The log configuration is controlled from the  `log4j2.xml` file under the resources sub-directory.
When running the script, 3 log files will be generated based on timestamp:
- `execution-{date}.log` -> Contains general logs of the execution
- `failedCsvLines-{date}.log` -> Contains a String representation of the csv lines that failed
- `failedCsvLinesDatasetAlreadyExists-{date}.log` -> Contais information of datasets that already existed in the system during creation
- `leadingZerosDatasetIds-{date}.log` -> Contains dataset ids, one per line, that have leading zeros
- `successfulDatasetIds-{date}.log` -> Contains all the successful identifiers, one per line. This can be used on the DELETE mode to delete everything that was added without impacting the rest of the database

