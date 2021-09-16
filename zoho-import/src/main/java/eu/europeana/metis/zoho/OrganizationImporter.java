package eu.europeana.metis.zoho;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;
import org.apache.commons.lang3.StringUtils;

import com.zoho.crm.api.record.DeletedRecord;
import com.zoho.crm.api.record.Record;

import eu.europeana.metis.utils.Constants;
import eu.europeana.metis.zoho.exception.OrganizationImportException;
import eu.europeana.metis.zoho.model.DeleteOperation;
import eu.europeana.metis.zoho.model.Operation;
import eu.europeana.metis.zoho.model.UpdateOperation;

/**
 * This class performs the import of organizations from Zoho to Metis. The import type is mandatory
 * as first command line argument, using one of {@link Constants#IMPORT_FULL}, {@link Constants#IMPORT_INCREMENTAL},
 * {@link Constants#IMPORT_INDIVIDUAL} or {@link Constants#IMPORT_DATE} If type is {@link Constants#IMPORT_DATE} second
 * argument needs to be a date provided in Zoho time format, see
 * {@link Constants#ZOHO_TIME_FORMAT}. If type is {@link Constants#IMPORT_INDIVIDUAL}, the second
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
        case Constants.IMPORT_FULL:
          lastRun = new Date(0);
          importer.fullImport = true;
          break;
        case Constants.IMPORT_INCREMENTAL:
          importer.incrementalImport = true;
          break;
        case Constants.IMPORT_DATE:
          if (args.length == 1) {
            logAndExit("A date must be provided when import type is: " + Constants.IMPORT_DATE);
          }

          lastRun = parseDate(args[1]);
          break;
        case Constants.IMPORT_INDIVIDUAL:
          if (args.length == 1) {
            logAndExit(
                "The id (uri) needs to be provided as command line parameter when import type is: "
                    + Constants.IMPORT_INDIVIDUAL);
          }
          importer.individualImport = true;
          importer.individualEntityId = args[1];
          if (!importer.individualEntityId.startsWith(Constants.URL_ORGANIZATION_PREFFIX)) {
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
    } catch (Exception th) {
      logAndExit("Cannot initialize importer!", th);
    }

    try {
      importer.run(lastRun);
    } catch (Exception th) {
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
  
  public void run(Date lastRun) throws ZohoException, OrganizationImportException {
    
    Date modifiedSince = lastRun;
    if (incrementalImport || individualImport) {
      lastRun = enrichmentService.getDateOfLastUpdatedOrganization();
      //EA-1466 lastModified is inclusive, add one second to avoid re-import of last modified entity
      long lastRunTimestamp = 0;
      if(lastRun != null) {             
          lastRunTimestamp = lastRun.getTime();
      }
      int oneSecond = 1000;
      modifiedSince = new Date(lastRunTimestamp + oneSecond);
    }

    List<Record> orgList ;
    SortedSet<Operation> operations;

    int page = 1;
    final int pageSize = 100;
    boolean hasNext = true;
    // Zoho doesn't return the number of organizations in get response.
    while (hasNext) {
      // retrieve modified organizations
      if (individualImport) {
        orgList = getOneOrganizationAsList(individualEntityId);
        hasNext = false;
      } else {
        OffsetDateTime offsetDateTime = modifiedSince.toInstant()
                .atOffset(ZoneOffset.UTC);
        orgList = zohoAccessClient.getZcrmRecordOrganizations(page, pageSize, offsetDateTime, searchCriteria, null);
        int start = (page-1)*pageSize;
		int end = start+orgList.size();
		LOGGER.info("Processing organizations set: {} - {}", start, end);
      }
      // collect operations to be run on Metis and Entity API
      operations = fillOperationsSet(orgList);
      // perform operations on all systems
      performOperations(operations);

      if (orgList.size() < pageSize) {
        // last page: if no more organizations exist in Zoho
        // TODO: there is the "more_records":false flag in the zoho response, we should use it
        hasNext = false;
      } else {
        // go to next page
        page++;
      }
    }
    // log status
    LOGGER.info("Processed update operations: {}", getStatus());
    // process organizations deleted in Zoho
    synchronizeDeletedZohoOrganizations(lastRun);
    // log status
    LOGGER.info("Processed delete operations: {}", getStatus());
  }

  /**
   * Returns the single Zoho Organisation as List
   * @param entityId
   * @return
   * @throws ZohoException
   */
  List<Record> getOneOrganizationAsList(String entityId)
      throws ZohoException {
    List<Record> res = new ArrayList<>();
    String zohoId = entityId.substring(Constants.URL_ORGANIZATION_PREFFIX.length());
    Optional<Record> organisation = zohoAccessClient.getZohoRecordOrganizationById(zohoId);
    if (organisation.isEmpty()) {
      throw new ZohoException("There is no zoho Organisation with id "+ zohoId);
    }
    //use for debugging purposes
//    System.out.println((new Gson()).toJson(organisation));
    res.add(organisation.get());
    return res;
  }

  /**
   * Performs Operations
   * For ACTION_UPDATE : converts the zoho Record to OrganisationEnrichmentEntity
   *                     and Enriches the wikidata
   * Always performs Metis operations ( if present in operations Set)
   *
   * @param operations
   * @throws OrganizationImportException
   */
  void performOperations(SortedSet<Operation> operations) throws OrganizationImportException {
    for (Operation op : operations) {
      // TODO: exception handling
      if (Constants.ACTION_UPDATE.equals(op.getAction())) {
        convertToEnrichmentOrganization(op);
        enrichWithWikidata(op);
      }
      performMetisOperation(op);
    }
  }

  /**
   * Performs Metis Operations
   * For ACTION_DELETE : Delete the entity from metis (deletes the entity if exists)
   * For ACTION_UPDATE : Updates the entity in metis (store new and update organizations changed in Zoho)
   * NOTE : both are performed in the enrichmentTerm
   *
   * @param operation
   * @throws OrganizationImportException
   */
  void performMetisOperation(Operation operation) throws OrganizationImportException {
    try {
      String entityId = operation.getOrganizationId();
      if (Constants.ACTION_DELETE.equals(operation.getAction())) {
        deleteFromMetis(entityId);
      } else if (Constants.ACTION_UPDATE.equals(operation.getAction())) {
        updateInMetis(operation);
      }
    } catch (Exception ex) {
      throw new OrganizationImportException(operation, "performMetisOperation", ex);
    }
  }

  /**
   * Retrieve deleted in Zoho organizations and removed from the Enrichment database
   * 
   * @return the number of deleted from Enrichment database organizations
   * @throws ZohoException
   * @throws OrganizationImportException
   */
  public int synchronizeDeletedZohoOrganizations(Date lastRun)
      throws ZohoException, OrganizationImportException {

    // do not delete organizations for individual entity importer
    // in case of full import the database should be manually cleaned. No need to delete organizations
    if (individualImport || fullImport)
      return 0;
    List<DeletedRecord> deletedRecordsInZoho;
    List<String> orgIdsDeletedInZoho = new ArrayList<>();
    int maxItemsPerPage = 200;
    int startPage = 1;
    boolean hasNext = true;
    int numberOfDeletedDbOrganizations = 0;
    List<String> toDelete;
    SortedSet<Operation> operations;

    // Zoho doesn't return the total results
    while (hasNext) {
      // list of (europeana) organizations ids
      deletedRecordsInZoho = zohoAccessClient.getZohoDeletedRecordOrganizations(startPage);
      // get the id list from Zoho deleted Record
      if (!deletedRecordsInZoho.isEmpty()) {
        deletedRecordsInZoho.forEach(deletedRecord -> orgIdsDeletedInZoho.add(Long.toString(deletedRecord.getId())));
      }
      // check exists in Metis (Note: zoho doesn't support filtering by lastModified for deleted
      // entities)
      toDelete = enrichmentService.findExistingOrganizations(orgIdsDeletedInZoho);
      // build delete operations set
      operations = fillDeleteOperationsSet(toDelete, lastRun);
      // execute delete operations
      performOperations(operations);

      // END LOOP: if no more organizations exist in Zoho
      if (orgIdsDeletedInZoho.size() < maxItemsPerPage)
        hasNext = false;

      // go next page
      startPage += 1;
    }

    return numberOfDeletedDbOrganizations;
  }

  /**
   * This method performs synchronization of organizations between Zoho and Entity API database
   * addressing deleted and unwanted (defined by owner criteria) organizations. We separate to
   * update from to delete types
   *
   * If full import -> only update/create
   * else Update operation -> 1) search criteria is empty, check if the owner is present in the zoho record
   *             2) search criteria present, then zoho record owner should match with
   *               the search filter ZOHO_OWNER_CRITERIA
   * Delete the record if none of the above matches.
   * 
   * @param orgList The list of retrieved Zoho objects
   */
  protected SortedSet<Operation> fillOperationsSet(final List<Record> orgList) {
    SortedSet<Operation> ret = new TreeSet<>();
    Operation operation;
    for (Record org : orgList) {
      // if full import then always update no deletion required
      if (fullImport){
        operation = new UpdateOperation(org);
      }
      // validate Zoho organization ownership
      else if (hasRequiredOwnership(org)) {
        // create or update organization
        operation = new UpdateOperation(org);
      } else {
        // add organization to the delete
        operation = new DeleteOperation(Long.toString(org.getId()), new Date(org.getModifiedTime().toEpochSecond()));
        // the organization doesn't have the
        LOGGER.info("The organization {} will be deleted as it doesn't have the required ownership anymore. organization owner: {}", org.getId(),
                getOrganisationConverter().getOwnerName(org));
      }
      ret.add(operation);
    }
    return ret;
  }

  private SortedSet<Operation> fillDeleteOperationsSet(List<String> orgIds, Date lastRun) {
    SortedSet<Operation> ret = new TreeSet<>();
    Operation operation;
    for (String orgId : orgIds) {
      operation = new DeleteOperation(orgId, lastRun);
      ret.add(operation);
    }
    return ret;
  }
}
