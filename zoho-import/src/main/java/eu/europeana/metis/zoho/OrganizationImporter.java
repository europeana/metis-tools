package eu.europeana.metis.zoho;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.corelib.definitions.edm.entity.Organization;
import eu.europeana.enrichment.service.EntityService;
import eu.europeana.enrichment.service.exception.ZohoAccessException;
import eu.europeana.enrichment.service.zoho.ZohoAccessService;
import eu.europeana.metis.authentication.dao.ZohoAccessClientDao;

/**
 * This class performs the import of organizations from Zoho to Metis.
 * 
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
		try {
			importer.init();
		} catch (Exception e) {
			LOGGER.error("Cannot initialize importer!", e);
			return;//the job cannot be run
		}
			
		try {
			importer.run();
		} catch (Exception exception) {
			LOGGER.error("The import job failed!" + exception);
			LOGGER.info("Successfully imported organizations: " + importer.importedOrgs);
		}
	}

	public void run() throws ZohoAccessException {
		
		List<Organization> orgList;
		int start = 1;
		final int rows = 100;
		boolean hasNext = true;
		//zoho doesn't return the number of organizations in get response. 
		while(hasNext){
			orgList = zohoAccessService.getOrganizations(start, rows);
			importedOrgs += storeOrganizations(orgList);
			start += rows;
			//if no more orgaizations exist in Zoho 
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
		ZohoAccessClientDao zohoAccessClientDao = new ZohoAccessClientDao(appProps.getProperty("zoho.base.url"),
				appProps.getProperty("zoho.base.authentication.token"));
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
