package eu.europeana.metis.zoho;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import eu.europeana.enrichment.api.external.model.zoho.ZohoOrganization;
import eu.europeana.enrichment.service.exception.ZohoAccessException;
import eu.europeana.enrichment.service.zoho.ZohoAccessService;
import eu.europeana.metis.authentication.dao.ZohoApiFields;
import eu.europeana.metis.zoho.exception.OrganizationImportException;
import eu.europeana.metis.zoho.model.DeleteOperation;
import eu.europeana.metis.zoho.model.Operation;
import eu.europeana.metis.zoho.model.UpdateOperation;
import eu.europeana.metis.zoho.python.SolrDocGeneratorPy;

/**
 * This class performs the import of organizations from Zoho to Metis. The import type is mandatory
 * as first command line argument, using one of {@link #IMPORT_FULL}, {@link #IMPORT_INCREMENTAL},
 * {@link #IMPORT_INDIVIDUAL} or {@link #IMPORT_DATE} If type is {@link #IMPORT_DATE} second
 * argument needs to be a date provided in Zoho time format, see
 * {@link ZohoApiFields#ZOHO_TIME_FORMAT}. If type is {@link #IMPORT_INDIVIDUAL}, the second
 * parameter must be the id (URI) of the entity
 * 
 * @author GordeaS
 *
 */
public class OrganizationImporter extends BaseOrganizationImporter {

  boolean incrementalImport = false;
  boolean individualImport = false;
  boolean fullImport = false;
  
  String individualEntityId;

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
          importer.fullImport = true;
          break;
        case IMPORT_INCREMENTAL:
          importer.incrementalImport = true;
          break;
        case IMPORT_DATE:
          if (args.length == 1) {
            logAndExit("A date must be provided when import type is: " + IMPORT_DATE);
          }

          lastRun = parseDate(args[1]);
          break;
        case IMPORT_INDIVIDUAL:
          if (args.length == 1) {
            logAndExit(
                "The id (uri) needs to be provided as command line parameter when import type is: "
                    + IMPORT_INDIVIDUAL);
          }
          importer.individualImport = true;
          importer.individualEntityId = args[1];
          if (!importer.individualEntityId.startsWith(ZohoAccessService.URL_ORGANIZATION_PREFFIX)) {
            logAndExit("Invalid entity id (uri). Entity id must start with: "
                + "http://data.europeana.eu");
          }
          break;

        default:
          logAndExit(
              "Invalid import type. Check command line arguments: " + StringUtils.join(args, ' '));
      }
    } else {
      logAndExit(
          "The import type is mandatory! Please provide one of the following as command like argument: "
              + args);
    }


    try {
      importer.init();
    } catch (Throwable th) {
      logAndExit("Cannot initialize importer!", th);
    }

    try {
      importer.run(lastRun);
    } catch (Throwable th) {
      LOGGER.info("Import failed with status: {}", importer.getStatus());
      logAndExit("The import job failed!", th);
    }
  }

  private static void logAndExit(String message, Throwable th) {
    if(th == null){
      LOGGER.error(message);
    } else {
      LOGGER.error(message, th);
    }
    // jenkins job failure is indicated trough a predefined value of the exit code, we set it too 3
    // (same as for runtime exceptions)
    System.exit(3);
  }

  private static void logAndExit(String message) {
    logAndExit(message, null);
  }
  
  public void run(Date lastRun) throws ZohoAccessException, OrganizationImportException {
    
    Date modifiedSince = lastRun;
    if (incrementalImport || individualImport) {
      lastRun = entityService.getLastOrganizationImportDate();
      //EA-1466 lastModified is inclusive, add one second to avoid re-import of last modified entity
      long lastRunTimestamp = 0;
      if(lastRun != null) {             
          lastRunTimestamp = lastRun.getTime();
      }
      int one_second = 1000;          
      modifiedSince = new Date(lastRunTimestamp + one_second);
    }

    List<ZohoOrganization> orgList;
    SortedSet<Operation> operations;

    int start = 1;
    final int rows = 100;
    boolean hasNext = true;
    // Zoho doesn't return the number of organizations in get response.
    while (hasNext) {
      // retrieve modified organizations
      if (individualImport) {
        orgList = getOneOrganizationAsList(individualEntityId);
        hasNext = false;
      } else {
        orgList = zohoAccessService.getOrganizations(start, rows, modifiedSince, searchCriteria);
        LOGGER.debug("Processing organizations set: {}", ""+start+"-"+(start+orgList.size()));
      }
      // collect operations to be run on Metis and Entity API
      operations = fillOperationsSet(orgList);
      // perform operations on all systems
      performOperations(operations);

      if (orgList.size() < rows) {
        // last page: if no more organizations exist in Zoho
        // TODO: there is the "more_records":false flag in the zoho response, we should use it
        hasNext = false;
      } else {
        // go to next page
        start += rows;
      }
    }
    // log status
    LOGGER.info("Processed update operations: {}", getStatus());

    // process organizations deleted in Zoho
    synchronizeDeletedZohoOrganizations(lastRun);
    // log status
    LOGGER.info("Processed delete operations: {}", getStatus());
  }

  List<ZohoOrganization> getOneOrganizationAsList(String entityId)
      throws ZohoAccessException {
    List<ZohoOrganization> res = new ArrayList<ZohoOrganization>();
    String zohoId = entityId.substring(ZohoAccessService.URL_ORGANIZATION_PREFFIX.length());
    res.add(zohoAccessService.getOrganization(zohoId));
    return res;
  }

  void performOperations(SortedSet<Operation> operations) throws OrganizationImportException {
    for (Operation op : operations) {
      // TODO: exception handling
      if (Operation.ACTION_UPDATE.equals(op.getAction())) {
        // convert to edmOrganization
        convertToEdmOrganization(op);
        // enrich with Wikidata
        enrichWithWikidata(op);
      }
      // update Metis
      performMetisOperation(op);
      // update Entity Api
      performEntityApiOperation(op);
    }
  }

  void performMetisOperation(Operation operation) throws OrganizationImportException {
    try {
      String entityId = operation.getEdmOrganizationId();
      if (Operation.ACTION_DELETE.equals(operation.getAction())) {
        deleteFromMetis(entityId);
      } else if (Operation.ACTION_UPDATE.equals(operation.getAction())) {
        // store new and update organizations changed in Zoho
        updateInMetis((UpdateOperation) operation);
      }
    } catch (Exception ex) {
      // convert runtime to catched exception to log failed operation
      throw new OrganizationImportException(operation, "performMetisOperation", ex);
    }
  }

  void performEntityApiOperation(Operation operation) throws OrganizationImportException {
    try {
      String entityId = operation.getEdmOrganizationId();
      if (Operation.ACTION_DELETE.equals(operation.getAction())) {
        // delete and commit
        getEntitySolrImporter().delete(entityId, true);
        getStatus().incrementdeletedEntityApi();
      } else if (Operation.ACTION_UPDATE.equals(operation.getAction())) {
        updateInEntityApi(entityId);
        getStatus().incrementImportedEntityApi();
      }
    } catch (Exception ex) {
      // convert runtime to catched exception to log failed operation
      throw new OrganizationImportException(operation, "performEntityApiOperation", ex);
    }
  }

  void updateInEntityApi(String entityId) throws Exception {
    getEntitySolrImporter().delete(entityId, false);
    // generate solrDoc
    File xmlFile = generateSolrDoc(entityId);
    // update solrDoc
    getEntitySolrImporter().add(xmlFile, true);
  }

  protected File generateSolrDoc(String entityId) throws Exception {
    SolrDocGeneratorPy generator =
        new SolrDocGeneratorPy(getProperty(OrganizationImporter.PROP_PYTHON),
            getProperty(OrganizationImporter.PROP_PYTHON_PATH),
            getProperty(OrganizationImporter.PROP_PYTHON_SCRIPT),
            getProperty(OrganizationImporter.PROP_PYTHON_WORKDIR));
    return generator.generateSolrDoc(entityId);
  }

  /**
   * Retrieve deleted in Zoho organizations and removed from the Enrichment database
   * 
   * @return the number of deleted from Enrichment database organizations
   * @throws ZohoAccessException
   * @throws OrganizationImportException
   */
  public int synchronizeDeletedZohoOrganizations(Date lastRun)
      throws ZohoAccessException, OrganizationImportException {

    // do not delete organizations for individual entity importer
    // in case of full import the database should be manually cleaned. No need to delete organizations
    if (individualImport || fullImport)
      return 0;

    List<String> orgIdsDeletedInZoho;
    int MAX_ITEMS_PER_PAGE = 200;
    int startPage = 1;
    boolean hasNext = true;
    int numberOfDeletedDbOrganizations = 0;
    List<String> toDelete;
    SortedSet<Operation> operations;

    // Zoho doesn't return the total results
    while (hasNext) {
      // list of (europeana) organizations ids
      orgIdsDeletedInZoho = zohoAccessService.getDeletedOrganizations(startPage);
      // check exists in Metis (Note: zoho doesn't support filtering by lastModified for deleted
      // entities)
      toDelete = entityService.findExistingOrganizations(orgIdsDeletedInZoho);
      // build delete operations set
      operations = fillDeleteOperationsSet(toDelete, lastRun);
      // execute delete operations
      performOperations(operations);

      // END LOOP: if no more organizations exist in Zoho
      if (orgIdsDeletedInZoho.size() < MAX_ITEMS_PER_PAGE)
        hasNext = false;

      // go next page
      startPage += 1;
    }

    return numberOfDeletedDbOrganizations;
  }

  /**
   * This method performs synchronization of organizations between Zoho and Entity API database
   * addressing deleted and unwanted (defined by search criteria) organizations. We separate to
   * update from to delete types
   * 
   * @param orgList The list of retrieved Zoho objects
   */
  protected SortedSet<Operation> fillOperationsSet(final List<ZohoOrganization> orgList) {

    SortedSet<Operation> ret = new TreeSet<Operation>();
    Operation operation;
    for (ZohoOrganization org : orgList) {
      // validate Zoho organization roles (workaround for Zoho API bug on role filtering)
      if (hasRequiredRole(org)) {
        // create or update organization
        operation = new UpdateOperation(org);
      } else {
        // add organization to the delete
        // toDeleteList.add(ZohoAccessService.URL_ORGANIZATION_PREFFIX + org.getZohoId());
        operation = new DeleteOperation(org.getZohoId(), org.getModified());
        // the organization doesn't have the
        LOGGER.info("{}",
            "The organization " + org.getZohoId()
                + " will be deleted as it doesn't have the required roles anymore. "
                + "organization role: " + org.getRole());
      }
      ret.add(operation);
    }

    return ret;
  }

  private SortedSet<Operation> fillDeleteOperationsSet(List<String> orgIds, Date lastRun) {
    SortedSet<Operation> ret = new TreeSet<Operation>();
    Operation operation;
    for (String orgId : orgIds) {
      operation = new DeleteOperation(orgId, lastRun);
      ret.add(operation);
    }
    return ret;
  }
}
