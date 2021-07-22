package eu.europeana.metis.zoho.model;

import java.util.Date;

import com.zoho.crm.api.record.Record;
import eu.europeana.enrichment.internal.model.OrganizationEnrichmentEntity;
import eu.europeana.metis.utils.Constants;

public class DeleteOperation extends BaseOperation{

   String zohoId;
   String organizationId;
   Date modified;
  
  public DeleteOperation(String zohoId, Date modified){
    this.zohoId=zohoId;
    this.modified = modified;
    this.organizationId = zohoId;
    setAction(Constants.ACTION_DELETE);
  }

  public String getZohoId() {
    return zohoId;
  }

  @Override
  public String getOrganizationId() {
    return organizationId;
  }

  @Override
  public Date getModified() {
    return modified;
  }

  @Override
  public OrganizationEnrichmentEntity getOrganisationEnrichmentEntity() {
    // Not used in delete operations
    return null;
  }

  @Override
  public void setOrganisationEnrichmentEntity(OrganizationEnrichmentEntity organizationEnrichmentEntity) {
    // Not used in delete operation
  }

  @Override
  public Record getZohoOrganization() {
    // Not used in delete operations
    return null;
  }
}
