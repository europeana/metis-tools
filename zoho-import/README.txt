The class eu.europeana.metis.zoho.OrganizationImporter contains a main method. 
The main method requires the optional parameter for importing only organizations created or modified after the provided date formatted as  
This class performs the import of organizations from Zoho to Metis.
 * The import type is mandatory as first command line argument, allowed values are "full/incremental/date"   
 * If type is "date" second argument needs to be a date provided in Zoho time format "yyyy-MM-dd hh:mm:ss" (e.g. 2018-03-12 10:07:22)

The property file zoho_import.properties is expected to be in the root of the classpath (i.e. resource directory, see 
example file). 