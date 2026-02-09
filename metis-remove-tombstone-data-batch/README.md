# Tool remove tombstone records from Metis tombstone database  
This project contains functionality to remove records from metis tombstone database.

### Updating configuration  
A `application.properties` file should be created under `src/main/resources` and the relative parameters should be populated.  

- `mode` -> The mode of the script: DEFAULT or DRY_RUN.
- `batchChunkSize` -> The batch size of records to be removed.

- `mongo` -> Mongo connection parameters.
- ```
  mongo.hosts=
  mongo.ports=
  mongo.authenticationDB=
  mongo.username=
  mongo.password=
  mongo.enableSSL=false
  mongo.tombstoneDB=
  mongo.connectionPoolSize=
  ```
- `solr` -> Solr connection parameters.
  ``` 
- solr.hosts= 
  ```
- `zookeeper` -> Zookeeper connection parameters.
  ``` 
  zookeeper.hosts=
  zookeeper.ports=
  zookeeper.chroot=
  zookeeper.defaultCollection=
  ```

### Running the script
It can be run either directly from the IDE by updating the `application.properties` file under `src/main/resources`.  
Or it can be build `mvn package` and a `*.jar` will be generated to run it independently.  
If it is run as a .jar, the `application.properties` file should be available on the same location where the .jar is.
include also the file `tombstone-data.csv` that contains the list of records to be removed, 
with the following comma separated values format:
```
europeana_id, resource_url
```
The log configuration is controlled from the  `log4j2.xml` file under the resources sub-directory.  

When running the script, log files will be generated based on timestamp:
- `execution-{date}.log` -> Contains general logs of the execution

Example command:  
`java -Dlog4j.configurationFile=log4j2.xml -jar metis-remove-tombstone-data-batch-1.0-SNAPSHOT.jar`
