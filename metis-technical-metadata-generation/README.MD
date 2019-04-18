**Tool to generate technical metadata of resources**  
This project contains functionality to generate technical metadata from provided resource urls.  
The resources urls should be provided in a directory and that directory should contain files, one file per dataset,
and each file should have a resource url per line.

**Updating configuration**  
A `application.properties` file should be created under `src/main/resources` and the relative parameters should be populated.  
Most of the fields are self explanatory and the ones that require some attention are:
- `directory.with.resources.per.dataset.path` -> The directory where all the files per dataset are
- `start.from.beginning.of.files` -> Should the script start from the beginning of the each file. Only try the ones that are not generated already and bypassing the failed ones.
This option should be considered only in the case that there are move resource urls added on an already processed file.
- `retry.failed.resources` -> Should the script retry the failed resource urls. 
This option will also move the pointer of the file at the beginning and read all lines but only reprocess the failed ones.
- `max.parallel.threads` -> The number of parallel threads. Each thread will take care of one dataset. Should be up to the total number of cpu cores.


If both `start.from.beginning.of.files` and `retry.failed.resources` are true then the functionality of `retry.failed.resources` precedes

**Running the script**  
It can be run either directly from the IDE by updating the `application.properties` file under `src/main/resources`.  
Or it can be build and a `*-jar-with-dependencies.jar` will be generated to run it independently.  
If it is run as a .jar, the `application.properties` file should be available on the same location where the .jar is.  
The log configuration is controlled from the  `log4j2.xml` file under the resources sub-directory.  
When running the script, log files will be generated based on timestamp:
- `execution-{date}.log` -> Contains general logs of the execution
- `statistics-{date}.log` -> Contains the final counts at the end of the execution
