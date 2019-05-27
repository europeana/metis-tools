**Tool reprocess all records**  
This project contains functionality to reprocess all provided source records.

**Updating configuration**  
A `application.properties` file should be created under `src/main/resources` and the relative parameters should be populated.  
Most of the fields are self explanatory and the ones that require some attention are:
- `max.parallel.threads` -> The number of parallel threads. Each thread will take care of one dataset. Should be up to the total number of cpu cores.
- `start.from.dataset.index` -> The index of the dataset in the ordered list from metis core, where the script should start from
- `end.at.dataset.index` -> The index of the dataset in the ordered list from metis core, where the script should end at(exclusive)
- `source.mongo.page.size` -> The page size when reading from mongo
- `mode` -> The mode of the script. DEFAULT, REPROCESS_ALL_FAILED
- `invalidate.plugin.types` -> The plugins in metis core to invalidate at the end of the reprocessing of a dataset
- `reprocess.based.on.plugin.type` -> The plugin where the reprocessed dataset in preview should be based upon 

**Checking progress**  
The script would create DatasetStatus file in the destination mongo, where all progress information of a dataset
will be contained. It will also generate FailedRecord entries in the destination mongo.  
For `mode` set to REPROCESS_ALL_FAILED, the reprocessing will start and the previously created DatasetStatus, 
will have it's failed record count decreasing and FailedRecord entries will be removed if successful.

**Running the script**  
It can be run either directly from the IDE by updating the `application.properties` file under `src/main/resources`.  
Or it can be build and a `*-jar-with-dependencies.jar` will be generated to run it independently.  
If it is run as a .jar, the `application.properties` file should be available on the same location where the .jar is.  
The log configuration is controlled from the  `log4j2.xml` file under the resources sub-directory.  
When running the script, log files will be generated based on timestamp:
- `execution-{date}.log` -> Contains general logs of the execution
- `statistics-{date}.log` -> Contains the final counts at the end of the execution

**Modifying implementation for a future reprocess operation**
The changes on the implementation, per reprocessing operation should be limited in the following files:  
- `PropertysHolderExtension` For the extra required properties from the properties file
- `ExtraConfiguration` For any extra fields that need to be initialized and the initialization of the required 
functional interfaces
- `ProcessingUtilities` That contains all functionality for processing records and datasets
- `IndexingUtilities` That contains all functionality for indexing records
- `AfterReProcessingUtilities` That contains all functionality for after reprocessing of all records, once per dataset  

Depending on the operation classes can be created and removed when not required anymore.

**Per reprocess operation information**  
<u>*Version 1(Summer 2019)*</u>  
The goal is to generate missing technical metadata for records that do not have them successfully generated in the past.  
In this case there was a Cache mongo with technical metadata pre-generated and S3 connection  
Extra classes created:  
- `CacheMongoDao` This is the cache dao that contains the pre-generated technical metadata
- `TechnicalMetadataWrapper` This class was copied from an other project to a package `eu.europeana.metis.technical.metadata.generation.model` 
so that morphia would not throw an error during reading
- `ThumbnailWrapper` This class was copied from an other project to a package `eu.europeana.metis.technical.metadata.generation.model` 
so that morphia would not throw an error during reading
