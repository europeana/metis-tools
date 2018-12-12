package eu.europeana.metis.zoho;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.europeana.corelib.definitions.edm.entity.Organization;
import eu.europeana.enrichment.api.external.model.zoho.ZohoOrganization;
import eu.europeana.enrichment.service.EntityService;
import eu.europeana.enrichment.service.WikidataAccessService;
import eu.europeana.enrichment.service.dao.WikidataAccessDao;
import eu.europeana.enrichment.service.dao.ZohoV2AccessDao;
import eu.europeana.enrichment.service.zoho.ZohoAccessService;
import eu.europeana.metis.authentication.dao.ZohoAccessClientDao;
import eu.europeana.metis.authentication.dao.ZohoApiFields;
import eu.europeana.metis.entity.EntityApiSolrImporter;
import eu.europeana.metis.zoho.exception.OrganizationImportException;
import eu.europeana.metis.zoho.model.ImportStatus;
import eu.europeana.metis.zoho.model.Operation;

public class BaseOrganizationImporter {

  public static final String PROP_ENTITY_API_SOLR_DOCS_FOLDER = "entity.api.solr.docs.folder";
  public static final String PROP_ENTITY_IMPORTER_SOLR_URL = "entity.importer.solr.url";
  public static final String PROP_MONGO_PORT = "mongo.port";
  public static final String PROP_MONGO_HOSTS = "mongo.hosts";
  public static final String PROP_ZOHO_ORGANIZATION_SEARCH_CRITERIA_ROLE = "zoho.organization.search.criteria.role";
  public static final String PROP_ZOHO_AUTHENTICATION_TOKEN = "zoho.authentication.token";
  public static final String PROP_ZOHO_BASE_URL_V2 = "zoho.base.url.v2";
  public static final String PROP_ZOHO_BASE_URL = "zoho.base.url";
  static final String PROP_PYTHON = "entity.importer.docs.generator.python";
  static final String PROP_PYTHON_PATH = "entity.importer.docs.generator.pythonpath";
  static final String PROP_PYTHON_SCRIPT = "entity.importer.docs.generator.pythonscript";
  static final String PROP_PYTHON_WORKDIR = "entity.importer.docs.generator.pythonworkdir";

  static final Logger LOGGER = LoggerFactory.getLogger(OrganizationImporter.class);
  Properties appProps;
  EntityService entityService;
  ZohoAccessService zohoAccessService;
  WikidataAccessService wikidataAccessService;
  EntityApiSolrImporter entitySolrImporter;
  ImportStatus status = new ImportStatus();

  static final String PROPERTIES_FILE = "/zoho_import.properties";
  
  public static final String IMPORT_FULL = "full";
  public static final String IMPORT_INCREMENTAL = "incremental";
  public static final String IMPORT_DATE = "date";
  String searchFilter;
  Map<String, String> searchCriteria = new HashMap<String, String>();
  Set<String> allowedRoles = new HashSet<String>();
  
 
  private void initSearchCriteria() {
    // build allowed roles
    if (StringUtils.isNotEmpty(searchFilter)) {
      LOGGER.info("apply filter for Zoho search criteria role: {}", searchFilter);
      searchCriteria.put(ZohoApiFields.ORGANIZATION_ROLE, searchFilter);
      //also init allowed roles, due to the Zoho bug on not using filtering (e.g. Provider, includes Data Provider)
      initAllowedRoles();
    }
  }

  private void initAllowedRoles() {
    String[] roles =
        searchFilter.split(ZohoApiFields.DELIMITER_COMMA);
    for (int i = 0; i < roles.length; i++) {
      allowedRoles.add(roles[i].trim());
    }
  }
  
  

  /**
   * This method validates that organization roles match to the filter criteria.
   * 
   * @param orgList
   * @return filtered and validated orgnization list
   */
  public boolean hasRequiredRole(ZohoOrganization organization) {

    boolean res = false;

    if (searchCriteria == null || searchCriteria.isEmpty())
      return true;

    // need to fix Zoho Bugg in API
    if (!searchCriteria.containsKey(ZohoApiFields.ORGANIZATION_ROLE))
      return true;

    if (organization.getRole() != null) {
      String[] organizationRoles = organization.getRole().split(ZohoApiFields.SEMICOLON);
      for (String organizationRole : organizationRoles) {
        if (allowedRoles.contains(organizationRole)) {
          res = true;
          break;
        } 
      }
    }

    return res;
  }

  protected void enrichWithWikidata(Operation operation) {
    Organization wikidataOrg;
    String wikidataUri = getWikidataUri(operation.getEdmOrganization());
    try {
      if (wikidataUri != null) {
        wikidataOrg = wikidataAccessService.dereference(wikidataUri);
        wikidataAccessService.mergePropsFromWikidata(operation.getEdmOrganization(), wikidataOrg);
      }
    } catch (Exception e) {
      LOGGER.warn("Cannot dereference organization from wikidata: {}", wikidataUri, e);
    }
  }

  private String getWikidataUri(Organization edmOrg) {
    String uri = null;
    if (edmOrg.getOwlSameAs() == null)
      return null;

    for (int i = 0; i < edmOrg.getOwlSameAs().length; i++) {
      uri = edmOrg.getOwlSameAs()[i];
      if (uri.startsWith(WikidataAccessService.WIKIDATA_BASE_URL)) {
        return uri;
      }
    }
    return null;
  }

  public void init() throws Exception {
    // read properties
    loadProperties(PROPERTIES_FILE);
    String zohoBaseUrl = getProperty(PROP_ZOHO_BASE_URL);
    String zohoBaseUrlV2 = getProperty(PROP_ZOHO_BASE_URL_V2);
    LOGGER.info("using zoho base URL: " + zohoBaseUrl);
    LOGGER.info("using zoho base URL V2: " + zohoBaseUrlV2);
    String token = getProperty(PROP_ZOHO_AUTHENTICATION_TOKEN);
    if (token == null || token.length() < 6)
      throw new IllegalArgumentException("zoho.authentication.token is invalid: " + token);
    LOGGER.info("using zoho zoho authentication token: " + token.substring(0, 3) + "...");

    // initialize filtering options
    searchFilter = getProperty(PROP_ZOHO_ORGANIZATION_SEARCH_CRITERIA_ROLE);
    initSearchCriteria();

    // initialize ZohoAccessService
    ZohoAccessClientDao zohoAccessClientDao = new ZohoAccessClientDao(zohoBaseUrl, token);
    ZohoV2AccessDao zohoV2AccessDao = new ZohoV2AccessDao(zohoBaseUrlV2, token);
    zohoAccessService = new ZohoAccessService(zohoAccessClientDao, zohoV2AccessDao);

    // initialize WikidataAccessService
    wikidataAccessService = new WikidataAccessService(new WikidataAccessDao());

    // initialize Metis EntityService
    String mongoHost = getProperty(PROP_MONGO_HOSTS);
    int mongoPort = Integer.valueOf(getProperty(PROP_MONGO_PORT));
    entityService = new EntityService(mongoHost, mongoPort);

    // initialize Entity API Solr Importer
    String solrUrl = getProperty(PROP_ENTITY_IMPORTER_SOLR_URL);
    String solrDocsFolderPath = getProperty(PROP_ENTITY_API_SOLR_DOCS_FOLDER);
    File solrDocsFolder = new File(solrDocsFolderPath);
    if(!solrDocsFolder.exists()){
      throw new IllegalArgumentException("Importer is not propertly initialized. The solr docs folder not found on file system: "+ solrDocsFolderPath);  
    }
    entitySolrImporter = new EntityApiSolrImporter(solrUrl);
    
  }

  protected Properties loadProperties(String propertiesFile)
      throws URISyntaxException, IOException, FileNotFoundException {
    appProps = new Properties();
    appProps.load( getClass().getResourceAsStream(propertiesFile));
    return appProps;
  }

  public EntityService getEntityService() {
    return entityService;
  }

  public ZohoAccessService getZohoAccessService() {
    return zohoAccessService;
  }

  /**
   * This method returns the classpath file for the give path name
   * 
   * @param fileName the name of the file to be searched in the classpath
   * @return the File object
   * @throws URISyntaxException
   * @throws IOException
   * @throws FileNotFoundException
   */
  protected File getClasspathFile(String fileName)
      throws URISyntaxException, IOException, FileNotFoundException {
    URL resource = getClass().getResource(fileName);
    if (resource == null) {
      LOGGER.info("Cannot classpath file: {}", fileName);
      return null;
    }
    URI fileLocation = resource.toURI();
    return (new File(fileLocation));
  }

  public ImportStatus getStatus() {
    return status;
  }
  
  protected void deleteFromMetis(String entityId) {
    // TODO: implement exists method
    Organization storedOrg = entityService.getOrganizationById(entityId);
    if (storedOrg != null) {
      entityService.deleteOrganization(entityId);
      // update import status
      getStatus().incrementdeletedMetis();
    } else {
      LOGGER.info("Cannot delete Organization, it was not found in Metis :{}", entityId);
    }
  }

  protected void convertToEdmOrganization(Operation op) throws OrganizationImportException {
    try {
      op.setEdmOrganization(zohoAccessService.toEdmOrganization(op.getZohoOrganization()));
    } catch (Exception ex) {
      //convert runtime to catched exception to log failed operation
      throw new OrganizationImportException(op, "convertToEdmOrganization", ex);
    }
  }

  protected void updateInMetis(Operation operation) {
    // create or update in Metis
    entityService.storeOrganization(operation.getEdmOrganization(),
        operation.getZohoOrganization().getCreated(), operation.getModified());
    //NOTE: at this point we could differentiate between create and update if needed
    status.incrementImportedMetis();
  }

  static Date parseDate(String dateString) {
    SimpleDateFormat format = new SimpleDateFormat(ZohoApiFields.ZOHO_TIME_FORMAT);
    try {
      return format.parse(dateString);
    } catch (ParseException e) {
      String message = "When first argument is: " + IMPORT_DATE
          + "the second argument must be a date formated as: " + ZohoApiFields.ZOHO_TIME_FORMAT;
      throw new IllegalArgumentException(message, e);
    }
  }

  public EntityApiSolrImporter getEntitySolrImporter() {
    return entitySolrImporter;
  }
  
  public String getProperty(String propertyName){
    return appProps.getProperty(propertyName);
  }
}
