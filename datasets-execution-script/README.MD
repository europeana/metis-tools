**Tool that handles execution of workflows using remote api calls**  
This project contain functionality to execute all dataset workflows in metis one after the other.  
Workflows should contain only one enabled plugin for execution.  
A dataset workflow will stop when its status reaches FINISHED, FAILED or CANCELLED.  
All processed datasetIds are stored in a log file and that log file is used at the beginning of the script to know which datasetIds have already been processed in a previous execution of the tool.  

**Updating configuration**  
A application.properties file should be created under src/main/resources and the relative parameters should be populated.
Most of the fields are self explanatory and the ones that require some attention are:
- `organization.id` -> Is the organizationId that will be used for all the workflows, it should be a real id from Zoho and it corresponds to the workflowOwner of the workflow
- `monitor.interval.in.secs` -> Is the number of seconds to wait before each monitor call to metis-core
- `enforced.plugin.type=` -> Can be used to enforce the source of plugin data for the execution. For example when running the PREVIEW plugin.
- `metis.core.host` -> Is the base url with which core would this tool communicate 
- `truststore.path/truststore.password` -> are used if the database to connect has self signed certificates and this trustore should contain that certificate
- `mongo.db` -> the database used to get the datasets from

**Running the script**
The log configuration is controlled from the  `log4j2.xml` file under the resources sub-directory.
When running the script, a log files will be generated based on timestamp:
- `execution-{date}.log` -> Contains general logs of the execution
- `final-dataset-status-{date}.log` -> Contains the final status of a dataset after its execution has ended
- `processed-datasets.log` -> Contains a dataset id per line. Each processed datasetId will be written here and this file is used on a subsequent execution to avoid re-running the same datasetIds again, in case the tool has to be restarted.  
If the execution has to start from the beginning, make sure to delete this file prior to execution.

**Building the script**
The tool can be run from any computer or a jar with dependecies can be created:  
`clean compile assembly:single` and then run:  
`java -jar datasets-execution-script-1.0-SNAPSHOT-jar-with-dependencies.jar`  
