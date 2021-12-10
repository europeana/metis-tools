**Tool to update total records for all datasets**  
This project contains functionality to update all total records for the latest indexing plugin preview/publish per dataset in metis core.  
This is accomplished by:
- Read all dataset ids from metis-core mongo
- Count number of records per dataset id and per indexing environment(preview/publish)
- Find the latest index preview, update the total database records value and save the relevant execution
- Find the latest index publish, update the total database records value and save the relevant execution