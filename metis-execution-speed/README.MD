**Tool to generate statistics of workflow execution steps speed**  
This project contains functionality to generate information about how fast execution steps in a workflow are.  
It reads from the WorkflowExecutions table in the specified environment/database to produce its results.   

**Updating configuration**  
A `application.properties` file should be created under `src/main/resources` and the relative parameters should be populated.  
Most of the fields are self explanatory and the ones that require some attention are:
- `start.number.of.days.ago` -> The number of days ago to start the calculation from
- `end.number.of.days.ago` -> The number of days ago to end the calculation at  
The calculation will be based on the beginning of the day of the starting date and the end of the day of the ending day.  
So an example with parameters `start.number.of.days.ago=2` and `end.number.of.days.ago=0`(today) the range would be dates of(UTC): 2019-03-23T00:00:00.000Z to 2019-03-25T23:59:59.999Z  

**Running the script**  
It can be run either directly from the IDE by updating the `application.properties` file under `src/main/resources`.  
Or it can be build and a `*-jar-with-dependencies.jar` will be generated to run it independently.  
If it is run as a .jar, the `application.properties` file should be available on the same location where the .jar is.  
The log configuration is controlled from the  `log4j2.xml` file under the resources sub-directory.  
When running the script, log files will be generated based on timestamp:
- `execution-{date}.log` -> Contains general logs of the execution
- `statistics-{date}.log` -> Contains the calculations structured  
