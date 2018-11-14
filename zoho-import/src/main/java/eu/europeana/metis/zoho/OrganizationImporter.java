package eu.europeana.metis.zoho;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import eu.europeana.enrichment.api.external.model.zoho.ZohoOrganization;
import eu.europeana.enrichment.service.exception.ZohoAccessException;
import eu.europeana.metis.authentication.dao.ZohoApiFields;
import eu.europeana.metis.zoho.exception.OrganizationImportException;
import eu.europeana.metis.zoho.model.DeleteOperation;
import eu.europeana.metis.zoho.model.Operation;
import eu.europeana.metis.zoho.model.UpdateOperation;

/**
 * This class performs the import of organizations from Zoho to Metis. The import type is mandatory
 * as first command line argument, using one of {@link #IMPORT_FULL}, {@link #IMPORT_INCREMENTAL} or
 * {@link #IMPORT_DATE} If type is {@link #IMPORT_DATE} second argument needs to be a date provided
 * in Zoho time format, see {@link ZohoApiFields#ZOHO_TIME_FORMAT}
 * 
 * @author GordeaS
 *
 */
public class OrganizationImporter extends BaseOrganizationImporter {

  boolean incrementalImport = false;

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
      LOGGER.info("Import failed with status: {}", importer.getStatus());
      LOGGER.error("The import job failed!", exception);
    }
  }

  public void run(Date lastRun) throws ZohoAccessException, OrganizationImportException {

    if (incrementalImport)
      lastRun = entityService.getLastOrganizationImportDate();

    List<ZohoOrganization> orgList;
    SortedSet<Operation> operations;

    int start = 1;
    final int rows = 100;
    boolean hasNext = true;
    // Zoho doesn't return the number of organizations in get response.
    while (hasNext) {
      // retrieve modified organizations
      orgList = zohoAccessService.getOrganizations(start, rows, lastRun, searchCriteria);
      // collect operations to be run on Metis and Entity API
      operations = fillOperationsSet(orgList);
      // perform operations on all systems
      performOperations(operations);

      if (orgList.size() < rows) {
        // last page: if no more organizations exist in Zoho
        //TODO: there is the "more_records":false flag in the zoho response, we should use it
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
      } else if (Operation.ACTION_UPDATE.equals(operation.getAction())) {
        updateInEntityApi(entityId);
      }
    } catch (Exception ex) {
      // convert runtime to catched exception to log failed operation
      throw new OrganizationImportException(operation, "performEntityApiOperation", ex);
    }
  }

  void updateInEntityApi(String entityId) throws SolrServerException, IOException {
    getEntitySolrImporter().delete(entityId, false);
    // generate solrDoc
    File xmlFile = generateSolrDoc(entityId);
    // update solrDoc
    getEntitySolrImporter().add(xmlFile, true);
  }


  private File generateSolrDoc(String entityId) {
    Process proc = null;
    try {
      proc = Runtime.getRuntime().exec("sh /home/shane/Documents/script.sh");
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return null;
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
