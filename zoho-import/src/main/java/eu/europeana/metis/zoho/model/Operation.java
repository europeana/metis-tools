package eu.europeana.metis.zoho.model;

import java.util.Date;

import com.zoho.crm.api.record.Record;
import eu.europeana.enrichment.internal.model.OrganizationEnrichmentEntity;

public interface Operation extends Comparable<Operation>{

  String getOrganizationId();

  String getZohoId();

  String getAction();

  Date getModified();

  OrganizationEnrichmentEntity getOrganisationEnrichmentEntity();

  void setOrganisationEnrichmentEntity(OrganizationEnrichmentEntity organizationEnrichmentEntity);

  Record getZohoOrganization();
}
