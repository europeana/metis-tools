**Tool to create a mapping between two databases for the workflow configuration regarding harvesting**  
This project contain functionality to create a backup of original workflows and update them with the europeana oai endpoints.
Once that is completed a migration of harvesting can be performed and once finished this tool has functionality to reverse to the original workflow information.  

**Updating configuration**  
A application.properties file should be created under src/main/resources and the relative parameters should be populated.
Most of the fields are self explanatory and the ones that require some attention are:
- `organization.id` -> Is the organizationId that will be used for all the workflows, it should be a real id from Zoho and it corresponds to the workflowOwner of the workflow
- `mode` -> Can be one of COPY_WORKFLOWS, CREATE_MAP, COPY_WORKFLOWS_AND_CREATE_MAP, REVERSE_MAP. More about modes below.   
After the CREATE_MAP execution, all datasets should be harvested and only then, the REVERSE_MAP should be executed to reach the previous state of the workflows 
- `truststore.path/truststore.password` -> are used if the database to connect has self signed certificates and this trustore should contain that certificate
- `mongo.db.original` -> this is the original database in the system with the original datasets and workflows, just after migration of datasets from the previous system(SugarCRM)
- `mongo.db.temporary` -> this is the temporary database used to copy workflows from the original database to store temporary until the harvesting of europeana endpoints is completed. Then this database is used to revert the workflows information in the original database.

**Modes**  
COPY_WORKFLOWS  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Copies all original workflows to a temporary database.  
CREATE_OAIPMH_WORKFLOWS, CREATE_PREVIEW_WORKFLOWS, CREATE_PUBLISH_WORKFLOWS  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Overwrites all original workflows with only one plugin(oaipmh and the corresponding europeana endpoint, preview, publish).      
REVERSE_WORKFLOWS  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Copies all workflows from the temporary database back to the original.  

**Running the script**
The log configuration is controlled from the  `log4j2.xml` file under the resources sub-directory.
When running the script, a log files will be generated based on timestamp:
- `execution-{date}.log` -> Contains general logs of the execution

**Getting a backup of the compose cluster in Bluemix**
mongodump --host "host.here" --port portNumber -u "user" -p "password" --authenticationDatabase "databaseToAuthenticate" --ssl --sslAllowInvalidCertificates --db "databaseToCreateDump"

**Restoring a backup of the compose cluster in Bluemix**
mongorestore --host "host.here" --port portNumber -u "user" -p "password" --authenticationDatabase "databaseToAuthenticate" --ssl --sslAllowInvalidCertificates --db "databaseToRestoreTheDumpTo" path/to/dump/directory

Creating and restoring a database described above was used for getting a backup of some real database to test autonomously.
And that was because the CREATE_MAP not only copies the original workflows to the temporary database, but also overwrites the workflows in the original database.  
