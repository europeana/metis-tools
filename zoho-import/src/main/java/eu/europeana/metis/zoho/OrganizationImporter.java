package eu.europeana.metis.zoho;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import eu.europeana.enrichment.service.exception.ZohoAccessException;
import eu.europeana.enrichment.service.zoho.ZohoAccessService;
import eu.europeana.metis.authentication.dao.ZohoAccessClientDao;
import eu.europeana.metis.authentication.dao.ZohoApiFields;

/**
 * This class performs the import of organizations from Zoho to Metis. The import type is mandatory
 * as first command line argument, using one of {@link #IMPORT_FULL}, {@link #IMPORT_INCREMENTAL} or
 * {@link #IMPORT_DATE} If type is {@link #IMPORT_DATE} second argument needs to be a date provided
 * in Zoho time format, see {@link ZohoApiFields#ZOHO_TIME_FORMAT}
 * 
 * @author GordeaS
 *
 */
public class OrganizationImporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(OrganizationImporter.class);
  private EntityService entityService;
  private ZohoAccessService zohoAccessService;
  private WikidataAccessService wikidataAccessService;
  
  private static final String PROPERTIES_FILE = "/zoho_import.properties";
  List<String> failedImports = new ArrayList<String>();
  int importedOrgs = 0;

  public static final String IMPORT_FULL = "full";
  public static final String IMPORT_INCREMENTAL = "incremental";
  public static final String IMPORT_DATE = "date";
  private boolean incrementalImport = false;
  private String searchFilter;
  Map<String, String> searchCriteria;
  Set<String> allowedRoles;

  /**
   * The main method performing the vocabulary mappings.
   * 
   * @param args the arguments. If the first argument exists it is assumed that it is an alternative
   *        location for the configuration file (see {@link MigrationProperties}).
   */
  public static void main(String[] args) {
    OrganizationImporter importer = new OrganizationImporter();

    Date lastRun = null;
    if (args.length > 0) {
      switch (args[0]) {
        case IMPORT_FULL:
          lastRun = new Date(0);
          break;
        case IMPORT_INCREMENTAL:
          importer.incrementalImport = true;
          break;
        case IMPORT_DATE:
          if (args.length == 1)
            throw new IllegalArgumentException(
                "A date must be provided when import type is: " + IMPORT_DATE);
          lastRun = parseDate(args[1]);
          break;
        default:
          throw new IllegalArgumentException(
              "Invalid import type. Check command line arguments: " + StringUtils.join(args, ' '));
      }
    } else {
      throw new IllegalArgumentException(
          "The import type is mandatory! Please provide one of the following as command like argument: "
              + args);
    }


    try {
      importer.init();
    } catch (Exception e) {
      LOGGER.error("Cannot initialize importer!", e);
      return;// the job cannot be run
    }

    try {
      importer.run(lastRun);
    } catch (Exception exception) {
      LOGGER.error("The import job failed!" + exception);
      LOGGER.info("Successfully imported organizations: {}", importer.importedOrgs);
    }
  }

  private static Date parseDate(String dateString) {
    SimpleDateFormat format = new SimpleDateFormat(ZohoApiFields.ZOHO_TIME_FORMAT);
    try {
      return format.parse(dateString);
    } catch (ParseException e) {
      String message = "When first argument is: " + IMPORT_DATE
          + "the second argument must be a date formated as: " + ZohoApiFields.ZOHO_TIME_FORMAT;
      throw new IllegalArgumentException(message, e);
    }
  }

  public void run(Date lastRun) throws ZohoAccessException {

    if (incrementalImport)
      lastRun = entityService.getLastOrganizationImportDate();

    List<ZohoOrganization> orgList;
    List<String> toDelete;
    List<ZohoOrganization> toUpdate;

    int start = 1;
    final int rows = 100;
    boolean hasNext = true;
    // Zoho doesn't return the number of organizations in get response.
    while (hasNext) {
      // start with clear list of deletions for each Zoho page
      toDelete = new ArrayList<>();
      // start with clear list of updates for each Zoho page
      toUpdate = new ArrayList<>();

      orgList = zohoAccessService.getOrganizations(start, rows, lastRun, searchCriteria);
      fillDeletionAndUpdateLists(orgList, toUpdate, toDelete);

      /* store new and update organizations changed in Zoho */
      importedOrgs += storeOrganizations(toUpdate);

      /* delete organizations that do not match filter criteria due to change role in Zoho */
      deleteOrganizations(toDelete);

      start += rows;
      // if no more organizations exist in Zoho
      if (orgList.size() < rows) {
        hasNext = false;
      }
    }

    LOGGER.info("Successfully imported organizations: {}", importedOrgs);

    int numberOfDeletedDbOrganizations = synchronizeDeletedZohoOrganizations();
    LOGGER.info("Successfully deleted from DB organizations: {}", numberOfDeletedDbOrganizations);
  }

  /**
   * This method performs synchronization of organizations between Zoho and Entity API database
   * addressing deleted and unwanted (defined by search criteria) organizations. We separate to
   * update from to delete types
   * 
   * @param orgList The list of retrieved Zoho objects
   * @param toUpdateList The list of Zoho organizations to update
   * @param toDeleteList The list of Zoho IDs to remove
   */
  private void fillDeletionAndUpdateLists(final List<ZohoOrganization> orgList,
      List<ZohoOrganization> toUpdateList, List<String> toDeleteList) {

    for (ZohoOrganization org : orgList) {
      // validate Zoho organization roles (workaround for Zoho API bug on role filtering)
      if(!hasRequiredRole(org)){
        //add organization to the delete
        toDeleteList.add(ZohoAccessService.URL_ORGANIZATION_PREFFIX + org.getZohoId());
      }else{
        //create or update organization
        toUpdateList.add(org);
      }
    }
  }

  /**
   * Retrieve deleted in Zoho organizations and removed from the Enrichment database
   * 
   * @return the number of deleted from Enrichment database organizations
   * @throws ZohoAccessException
   */
  public int synchronizeDeletedZohoOrganizations() throws ZohoAccessException {

    List<String> deletedZohoOrganizationList;
    int MAX_ITEMS_PER_PAGE = 200;
    int startPage = 1;
    boolean hasNext = true;
    int numberOfDeletedDbOrganizations = 0;
    while (hasNext) {
      deletedZohoOrganizationList = zohoAccessService.getDeletedOrganizations(startPage);
      numberOfDeletedDbOrganizations += deleteOrganizations(deletedZohoOrganizationList);
      startPage += 1;
      // if no more organizations exist in Zoho
      if (deletedZohoOrganizationList.size() < MAX_ITEMS_PER_PAGE)
        hasNext = false;
    }

    return numberOfDeletedDbOrganizations;
  }

  private Map<String, String> buildSearchCriteria() {
    Map<String, String> searchCriteria = null;
    if (StringUtils.isNotEmpty(searchFilter)) {
      searchCriteria = new HashMap<String, String>();
      LOGGER.info("apply filter for Zoho search criteria role: {}", searchFilter);
      searchCriteria.put(ZohoApiFields.ORGANIZATION_ROLE, searchFilter);
    }
    return searchCriteria;
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
        } else {
          LOGGER.warn("{}", "Ignoring organization " + organization.getZohoId()
              + "as the role doesn't match the role criteria: " + organization.getRole());
        }
      }
    }

    return res;
  }

  private int storeOrganizations(List<ZohoOrganization> orgList) {
    int count = 0;
    Organization edmOrg;
    
    for (ZohoOrganization org : orgList) {
      try {
          edmOrg = zohoAccessService.toEdmOrganization(org);
          enrichWithWikidata(edmOrg);      
          entityService.storeOrganization(edmOrg, org.getCreated(), org.getModified());
          count++;   
      } catch (Exception e) {
        LOGGER.warn("Cannot import organization: {}", org.getZohoId(), e);
        failedImports.add(org.getZohoId());
      }
    }
    return count;
  }

  private void enrichWithWikidata(Organization edmOrg){
    Organization wikidataOrg;
    String wikidataUri = getWikidataUri(edmOrg);
    try{
      if(wikidataUri != null){
        wikidataOrg = wikidataAccessService.dereference(wikidataUri);
        wikidataAccessService.mergePropsFromWikidata(edmOrg, wikidataOrg);    
      }
    }catch(Exception e){
      LOGGER.warn("Cannot dereference organization from wikidata: {}", wikidataUri, e);
    }
  }

  private String getWikidataUri(Organization edmOrg) {
    String uri = null;
    if(edmOrg.getOwlSameAs() == null)
      return null;
    
    for (int i = 0; i < edmOrg.getOwlSameAs().length; i++) {
      uri = edmOrg.getOwlSameAs()[i];
      if (uri.startsWith(WikidataAccessService.WIKIDATA_BASE_URL)){
        return uri;
      }
    }
    return null;
  }

  /**
   * This method removes organizations from database
   * using IDs from given list that exist in database
   * 
   * @param orgList The list of organizations to remove
   * @return The number of removed organizations
   */
  private int deleteOrganizations(List<String> orgList) {
    int count = 0;

    if (!orgList.isEmpty()) {
      try {
        // get organizations existing in database
        List<String> dbOrgList = entityService.findExistingOrganizations(orgList);
        if(!dbOrgList.isEmpty()) {
          entityService.deleteOrganizations(dbOrgList);
          count = dbOrgList.size(); 
        }
      } catch (Exception e) {
        LOGGER.warn("Cannot delete organizations. ", e);
      }
    }
    return count;
  }

  public void init() throws Exception {
    //read properties
    Properties appProps = loadProperties(PROPERTIES_FILE);
    String zohoBaseUrl = appProps.getProperty("zoho.base.url");
    String zohoBaseUrlV2 = appProps.getProperty("zoho.base.url.v2");
    LOGGER.info("using zoho base URL: " + zohoBaseUrl);
    LOGGER.info("using zoho base URL V2: " + zohoBaseUrlV2);
    String token = appProps.getProperty("zoho.authentication.token");
    if (token == null || token.length() < 6)
      throw new IllegalArgumentException("zoho.authentication.token is invalid: " + token);
    LOGGER.info("using zoho zoho authentication token: " + token.substring(0, 3) + "...");

    //initialize filtering options
    searchFilter = appProps.getProperty("zoho.organization.search.criteria.role");
    initSearchCriteria();
    
    //initialize ZohoAccessService
    ZohoAccessClientDao zohoAccessClientDao = new ZohoAccessClientDao(zohoBaseUrl, token);
    ZohoV2AccessDao zohoV2AccessDao = new ZohoV2AccessDao(zohoBaseUrlV2, token);
    zohoAccessService = new ZohoAccessService(zohoAccessClientDao, zohoV2AccessDao);
    
    //initialize WikidataAccessService
    wikidataAccessService = new WikidataAccessService(new WikidataAccessDao());

    //initialize EntityService
    String mongoHost = appProps.getProperty("mongo.hosts");
    int mongoPort = Integer.valueOf(appProps.getProperty("mongo.port"));
    entityService = new EntityService(mongoHost, mongoPort);
   
  }

  private void initSearchCriteria() {
    // build allowed roles
    searchCriteria = buildSearchCriteria();
    String[] roles =
        searchCriteria.get(ZohoApiFields.ORGANIZATION_ROLE).split(ZohoApiFields.DELIMITER_COMMA);
    allowedRoles = new HashSet<String>();
    for (int i = 0; i < roles.length; i++) {
      allowedRoles.add(roles[i].trim());
    }
  }

  protected Properties loadProperties(String propertiesFile)
      throws URISyntaxException, IOException, FileNotFoundException {
    Properties appProps = new Properties();
    File propfile = getClasspathFile(propertiesFile);
    appProps.load(new FileInputStream(propfile));
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
   * @param fileName the name of the file to be searched in the classpath
   * @return the File object 
   * @throws URISyntaxException
   * @throws IOException
   * @throws FileNotFoundException
   */
  protected File getClasspathFile(String fileName)
      throws URISyntaxException, IOException, FileNotFoundException {
    URL resource = getClass().getResource(fileName);
    if(resource == null){
      LOGGER.info("Cannot classpath file: {}", fileName);
      return null;
    }
    URI fileLocation = resource.toURI();
    return (new File(fileLocation));
  }

}
