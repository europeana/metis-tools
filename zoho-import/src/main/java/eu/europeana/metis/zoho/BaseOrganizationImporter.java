package eu.europeana.metis.zoho;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.zoho.crm.api.record.Record;
import eu.europeana.enrichment.internal.model.OrganizationEnrichmentEntity;
import eu.europeana.enrichment.service.EnrichmentService;
import eu.europeana.metis.config.OrganisationImporterConfig;
import eu.europeana.metis.utils.Constants;
import eu.europeana.metis.utils.OrganizationConverter;
import eu.europeana.metis.wiki.WikidataAccessDao;
import eu.europeana.metis.wiki.WikidataAccessService;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import eu.europeana.corelib.definitions.edm.entity.Organization;

import eu.europeana.metis.zoho.exception.OrganizationImportException;
import eu.europeana.metis.zoho.model.ImportStatus;
import eu.europeana.metis.zoho.model.Operation;

/**
 * Configuration for Organisation Importer Class
 *
 * @author Srishti Singh (srishti.singh@europeana.eu)
 * @since 2021-07-06
 */
public class BaseOrganizationImporter {

   static final Logger LOGGER = LogManager.getLogger(BaseOrganizationImporter.class);

  ZohoAccessClient zohoAccessClient;
  EnrichmentService enrichmentService ;
  WikidataAccessService wikidataAccessService;
  ImportStatus status = new ImportStatus();
  OrganisationImporterConfig organisationImporterConfig = new OrganisationImporterConfig();
  private final OrganizationConverter organisationConverter = new OrganizationConverter();
  String searchFilter;
  Map<String, String> searchCriteria = new HashMap<>();
  Set<String> allowedRoles = new HashSet<>();

  public OrganizationConverter getOrganisationConverter() {
    return organisationConverter;
  }

  public OrganisationImporterConfig getOrganisationImporterConfig() {
    return organisationImporterConfig;
  }

  public EnrichmentService getEnrichmentService() {
    return enrichmentService;
  }

  public ZohoAccessClient getZohoAccessClient() {
    return zohoAccessClient;
  }

  public WikidataAccessService getWikidataAccessService() {
    return wikidataAccessService;
  }

  public ImportStatus getStatus() {
    return status;
  }

  public void init() throws Exception {
    searchFilter = getOrganisationImporterConfig().getSearchFilter();
    initSearchCriteria();
    zohoAccessClient = getOrganisationImporterConfig().getZohoAccessClient();
    enrichmentService = getOrganisationImporterConfig().getEnrichmentService();
    wikidataAccessService = new WikidataAccessService(new WikidataAccessDao());
  }

  /**
   * Builds allowed roles
   * Also init allowed roles, due to the Zoho bug on not using filtering (e.g. Provider, includes Data Provider)
   */
  private void initSearchCriteria() {
    if (StringUtils.isNotEmpty(searchFilter)) {
      LOGGER.info("apply filter for Zoho search criteria role: {}", searchFilter);
      searchCriteria.put(ZohoConstants.ORGANIZATION_ROLE_FIELD, searchFilter);
      initAllowedRoles();
    }
  }

  private void initAllowedRoles() {
    String[] roles = searchFilter.split(ZohoConstants.DELIMITER_COMMA);
    for (int i = 0; i < roles.length; i++) {
      allowedRoles.add(roles[i].trim());
    }
  }

  /**
   * This method validates that organization roles match to the filter criteria.
   *
   * @param recordOrganization
   * @return filtered and validated orgnization list
   */
  public boolean hasRequiredRole(Record recordOrganization) {
    boolean res = false;
    List<String> organizationRoles = getOrganisationConverter().getOrganizationRole(recordOrganization);
    
	if (searchCriteria == null || searchCriteria.isEmpty()
			|| !searchCriteria.containsKey(ZohoConstants.ORGANIZATION_ROLE_FIELD)) {
		// EA-2623 consider only collections that have a europeana role assigned
		return CollectionUtils.isNotEmpty(organizationRoles);
	} else if (!organizationRoles.isEmpty()) {
		// check organization roles when a filter is used
		for (String organizationRole : organizationRoles) {
			if (allowedRoles.contains(organizationRole.trim())) {
				LOGGER.info("Organisation has the required role : {}", organizationRole);
				res = true;
				break;
			}
		}
	}
	return res;
  }

  protected void enrichWithWikidata(Operation operation) {
    Organization wikidataOrg;
    String wikidataUri = getWikidataUri(operation.getOrganisationEnrichmentEntity());
    try {
      if (wikidataUri != null) {
        LOGGER.info("Enriching with wikidata uri: {}", wikidataUri);
        wikidataOrg = wikidataAccessService.dereference(wikidataUri);
        wikidataAccessService.mergePropsFromWikidata(operation.getOrganisationEnrichmentEntity(), wikidataOrg);
      }
    } catch (Exception e) {
      LOGGER.warn("Cannot dereference organization from wikidata: {}", wikidataUri, e);
    }
  }

  public String getWikidataUri(OrganizationEnrichmentEntity organizationEnrichmentEntity) {
    if (organizationEnrichmentEntity.getOwlSameAs() == null)
      return null;

    for (int i = 0; i < organizationEnrichmentEntity.getOwlSameAs().size(); i++) {
      String uri = organizationEnrichmentEntity.getOwlSameAs().get(i);
      if (uri.startsWith(Constants.WIKIDATA_BASE_URL)) {
        return uri;
      }
    }
    return null;
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
  protected File getClasspathFile(String fileName) throws URISyntaxException {
    URL resource = getClass().getResource(fileName);
    if (resource == null) {
      LOGGER.info("Cannot classpath file: {}", fileName);
      return null;
    }
    URI fileLocation = resource.toURI();
    return (new File(fileLocation));
  }


  protected void deleteFromMetis(String entityId) {
    Optional<OrganizationEnrichmentEntity> storedOrg = enrichmentService.getOrganizationByUri(Constants.URL_ORGANIZATION_PREFFIX + entityId);
    if (storedOrg.isEmpty()) {
      LOGGER.info("Cannot delete Organization, it was not found in Metis :{}", entityId);
    } else {
      enrichmentService.deleteOrganization(entityId);
      LOGGER.info("Deleted Organization in Metis :{}", entityId);

      // update import status
      getStatus().incrementdeletedMetis();
    }
  }

  protected void convertToEnrichmentOrganization(Operation op) throws OrganizationImportException {
    try {
      op.setOrganisationEnrichmentEntity(getOrganisationConverter().convertToOrganizationEnrichmentEntity(op.getZohoOrganization()));
    } catch (Exception ex) {
      throw new OrganizationImportException(op, "convertToOrganizationEnrichmentEntity", ex);
    }
  }

  /**
   * Create or Updates in Metis
   * @param operation
   */
  protected void updateInMetis(Operation operation) {
    enrichmentService.saveOrganization(operation.getOrganisationEnrichmentEntity(),
        new Date(operation.getZohoOrganization().getCreatedTime().toEpochSecond()), operation.getModified());
    LOGGER.info("Organization saved in Metis :{}", operation.getOrganisationEnrichmentEntity().getAbout());

    //NOTE: at this point we could differentiate between create and update if needed
    status.incrementImportedMetis();
  }

  static Date parseDate(String dateString) {
    SimpleDateFormat format = new SimpleDateFormat(Constants.ZOHO_TIME_FORMAT);
    try {
      return format.parse(dateString);
    } catch (ParseException e) {
      String message = "When first argument is: " + Constants.IMPORT_DATE
          + "the second argument must be a date formated as: " + Constants.ZOHO_TIME_FORMAT;
      throw new IllegalArgumentException(message, e);
    }
  }
}
