**Tool to remove tombstone records**  
This project contains functionality to remove records from tombstone provided source records.

**Updating configuration**  
A `application.properties` file should be created under `src/main/resources` and the relative parameters should be populated.  
Most of the fields are self explanatory and the ones that require some attention are:
- `min.parallel.datasets` -> The number of minimum parallel datasets.
- `max.parallel.threads.per.dataset` -> The maximum number of threads a dataset can have, each thread processing a page.  
The multiplication of `min.parallel.datasets` with `max.parallel.threads.per.dataset` shows the total amount of threads the application will utilize.  
So for example if `min.parallel.datasets` 3 and `max.parallel.threads.per.dataset` 24, there can be maximum 72 datasets run in parallel with 1 thread available to them.  
Or 3 big datasets with 24 threads each. And any combination in between. The threads per dataset is calculated based on the page size and the total number of pages of the dataset.
For the REPROCESS_ALL_FAILED mode only parallelization of datasets is implemented so the `max.parallel.threads.per.dataset` should be 1 and the `min.parallel.datasets` should be set to the amount of parallel datasets.
- `start.from.dataset.index` -> The index of the dataset in the list from where the processing should start from. 0 if no value provided
- `end.at.dataset.index` -> The index of the dataset in the list from where the processing should end at. Max integer if no value provided
- `source.mongo.page.size` -> The page size when reading from mongo, keep the same on subsequent executions otherwise unexpected behavior might occur
- `mode` -> The mode of the script:  
  CLEAN mode is to be used to just clean the destination databases, it does no processing.   
  DEFAULT for a normal operation of process and index.  
  REPROCESS_ALL_FAILED will process only the failed records for a dataset even if it's not previously completely processed.
  POST_CLEAN mode is to be used to just remove the destination temporary databases, it does no processing.  
- `dataset.ids.to.process` -> A comma separated list of dataset ids to process bypassing processing all dataset ids.  
  If this field is empty then the metis core mongo configuration is required so that datasetIds are fetched from there. Otherwise it's not required.
- `identity.process` -> Set to true to do a simple read and then write reindex, without altering the record
- `clean.databases.before.process=` -> Set to true if we want to clean the database before we start a new process, used for DEFAULT mode only

**Checking progress**  
The script would create DatasetStatus entries in the destination mongo, where all progress information of a dataset
will be contained. It will also generate FailedRecord entries in the destination mongo.  
For `mode` set to REPROCESS_ALL_FAILED, the reprocessing will start and the previously created DatasetStatus 
for a dataset will have it's failed record count decreasing and any matching FailedRecord entries will be removed if successful.

**Running the script**  
It can be run either directly from the IDE by updating the `application.properties` file under `src/main/resources`.  
Or it can be build and a `*-jar-with-dependencies.jar` will be generated to run it independently.  
If it is run as a .jar, the `application.properties` file should be available on the same location where the .jar is.  
The log configuration is controlled from the  `log4j2.xml` file under the resources sub-directory.  
When running the script, log files will be generated based on timestamp:
- `execution-{date}.log` -> Contains general logs of the execution
- `statistics-{date}.log` -> Contains the final counts at the end of the execution

Example command:  
`java -Dlog4j.configurationFile=log4j2.xml -jar metis-depublish-tombstone-data-1.0-SNAPSHOT-jar-with-dependencies.jar`

**Modifying implementation for a future reprocess operation**
The changes on the implementation, per reprocessing operation should be limited in the following files:  
- `DefaultConfiguration` For any extra fields that need to be initialized and the initialization of the required 
functional interfaces. The processRDF method is used in turn in the ProcessUtilities and it's functionality can be modified from this file.
- `ProcessUtilities` That contains all functionality for processing records and datasets
- `IndexUtilities` That contains all functionality for removing tombstone records
  

Depending on the operation, classes can be created and removed when not required anymore.

**Per reprocess operation information**  
MET-6582 remove tombstone records initial script
