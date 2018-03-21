package eu.europeana.metis.zoho;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.corelib.definitions.edm.entity.Organization;
import eu.europeana.enrichment.service.EntityService;
import eu.europeana.enrichment.service.exception.ZohoAccessException;
import eu.europeana.enrichment.service.zoho.ZohoAccessService;
import eu.europeana.metis.authentication.dao.ZohoAccessClientDao;
import eu.europeana.metis.authentication.dao.ZohoApiFields;

/**
 * This class performs the import of organizations from Zoho to Metis.
 * The import type is mandatory as first command line argument, using one of {@link #IMPORT_FULL}, {@link #IMPORT_INCREMENTAL} or {@link #IMPORT_DATE}   
 * If type is {@link #IMPORT_DATE} second argument needs to be a date provided in Zoho time format, see {@link ZohoApiFields#ZOHO_TIME_FORMAT}
 * @author GordeaS
 *
 */
public class OrganizationImporter {

	private static final Logger LOGGER = LoggerFactory.getLogger(OrganizationImporter.class);
	private EntityService entityService;
	private ZohoAccessService zohoAccessService;
	private static final String PROPERTIES_FILE = "/zoho_import.properties";
	List<String> failedIports = new ArrayList<String>();
	int importedOrgs = 0;
	
	public static final String IMPORT_FULL = "full";
	public static final String IMPORT_INCREMENTAL = "incremental";
	public static final String IMPORT_DATE = "date";
	private boolean incrementalImport = false;
	
	/**
	 * The main method performing the vocabulary mappings.
	 * 
	 * @param args
	 *            the arguments. If the first argument exists it is assumed that
	 *            it is an alternative location for the configuration file (see
	 *            {@link MigrationProperties}).
	 */
	public static void main(String[] args) {
		OrganizationImporter importer = new OrganizationImporter();
		
		Date lastRun = null;
		if(args.length > 0){
			switch(args[0]){
				case IMPORT_FULL:
					lastRun = new Date(0);
					break;
				case IMPORT_INCREMENTAL:
					importer.incrementalImport = true;
					break;
				case IMPORT_DATE:
					if(args.length == 1)
						throw new IllegalArgumentException("A date must be provided when import type is: " + IMPORT_DATE);
					lastRun = parseDate(args[1]);
					break;
				default:
					throw new IllegalArgumentException("Invalid import type. Check command line arguments: " + StringUtils.join(args, ' '));			
			}
		}else{
			throw new IllegalArgumentException("The import type is mandatory! Please provide one of the following as command like argument: " 
					+ args);
		}
		
		
		try {
			importer.init();
		} catch (Exception e) {
			LOGGER.error("Cannot initialize importer!", e);
			return;//the job cannot be run
		}
			
		try {
			importer.run(lastRun);
		} catch (Exception exception) {
			LOGGER.error("The import job failed!" + exception);
			LOGGER.info("Successfully imported organizations: " + importer.importedOrgs);
		}
	}

	private static Date parseDate(String dateString) {
		SimpleDateFormat format = new SimpleDateFormat(ZohoApiFields.ZOHO_TIME_FORMAT);
		try {
			return format.parse(dateString);
		} catch (ParseException e) {
			String message = "When first argument is: " + IMPORT_DATE + "the second argument must be a date formated as: " + ZohoApiFields.ZOHO_TIME_FORMAT;
			throw new IllegalArgumentException(message, e); 
		}
	}

	public void run(Date lastRun) throws ZohoAccessException {
		
		if(incrementalImport)
			lastRun = entityService.getLastOrganizationImportDate();
		
		List<Organization> orgList;
		int start = 1;
		final int rows = 100;
		boolean hasNext = true;
		
	    Map<String,String> searchCriteria = new HashMap<String,String>();
	            
        try {
          Properties appProps = loadProperties(PROPERTIES_FILE);
          String zohoSearchCriteriaRole = appProps.getProperty("zoho.organization.search.criteria.role");
          if (StringUtils.isNotEmpty(zohoSearchCriteriaRole)) {
            LOGGER.info("apply filter for Zoho search criteria role: " + zohoSearchCriteriaRole);
            searchCriteria.put(ZohoApiFields.ORGANIZATION_ROLE, zohoSearchCriteriaRole);   
          }
        } catch (FileNotFoundException e) {
          throw new ZohoAccessException("Zoho import property file not found.", e);
        } catch (URISyntaxException e) {
          throw new ZohoAccessException("Zoho import syntax exception.", e);
        } catch (IOException e) {
          throw new ZohoAccessException("Zoho import IO exception.", e);
        }
		
		//Zoho doesn't return the number of organizations in get response. 
		while(hasNext){
			orgList = zohoAccessService.getOrganizations(start, rows, lastRun, searchCriteria);
			importedOrgs += storeOrganizations(orgList);
			start += rows;
			//if no more organizations exist in Zoho 
			if(orgList.size() < rows)
				hasNext = false;
		}

		LOGGER.info("Successfully imported organizations: " + importedOrgs);
	}

	private int storeOrganizations(List<Organization> orgList) {
		int count = 0;
		for (Organization org : orgList) {
			try{
				entityService.storeOrganization(org);
				count++;
			}catch(Exception e){
				LOGGER.warn("Cannot import organization: " + org.getAbout(), e);
				failedIports.add(org.getAbout());
			}
		}
		return count;
	}

	public void init() throws Exception {
		Properties appProps = loadProperties(PROPERTIES_FILE);
		String zohoBaseUrl = appProps.getProperty("zoho.base.url");
		LOGGER.info("using zoho base URL: " + zohoBaseUrl);
		String token = appProps.getProperty("zoho.authentication.token");
		if(token == null || token.length() < 6)
			throw new IllegalArgumentException("zoho.authentication.token is invalid: " + token);
		LOGGER.info("using zoho zoho authentication token: " + token.substring(0, 3) + "...");
		
		ZohoAccessClientDao zohoAccessClientDao = new ZohoAccessClientDao(zohoBaseUrl,
				token);
		
		zohoAccessService = new ZohoAccessService(zohoAccessClientDao);

		String mongoHost = appProps.getProperty("mongo.hosts");
		int mongoPort = Integer.valueOf(appProps.getProperty("mongo.port"));
		entityService = new EntityService(mongoHost, mongoPort);
	}

	protected Properties loadProperties(String propertiesFile)
			throws URISyntaxException, IOException, FileNotFoundException {
		Properties appProps = new Properties();
		URI propLocation = getClass().getResource(propertiesFile).toURI();
		appProps.load(new FileInputStream(new File(propLocation)));
		return appProps;
	}

	public EntityService getEntityService() {
		return entityService;
	}

	public ZohoAccessService getZohoAccessService() {
		return zohoAccessService;
	}

}
