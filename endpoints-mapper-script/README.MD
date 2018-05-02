**Tool to create a mapping between two databases for the workflow configuration regarding harvesting**  
This project contain functionality to create a backup of original workflows and update them with the europeana oai endpoints.
Once that is completed a migration of harvesting can be performed and once finished this tool has functionality to reverse to the original workflow information.  

**Updating configuration**  
A application.properties file should be created under src/main/resources and the relative parameters should be populated.
Most of the fields are self explanatory and the ones that require some attention are:
- `organization.id` -> Is the organizationId that will be used for all the workflows, it should be a real id from Zoho and it corresponds to the workflowOwner of the workflow
- `mode` -> Can be either CREATE_MAP for copying the original workflows and updating the original datbase with workflows that contain the europeana oai endpoints, or REVERSE_MAP to copy the workflows from the temporary database to the original one  
After the CREATE_MAP execution, all datasets should be harvested and only then, the REVERSE_MAP should be executed to reach the previous state of the workflows 
- `truststore.path/truststore.password` -> are used if the database to connect has self signed certificates and this trustore should contain that certificate

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
