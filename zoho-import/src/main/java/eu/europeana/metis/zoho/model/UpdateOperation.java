package eu.europeana.metis.zoho.model;

import java.util.Date;

import com.zoho.crm.api.record.Record;
import eu.europeana.enrichment.internal.model.OrganizationEnrichmentEntity;
import eu.europeana.metis.utils.Constants;
import eu.europeana.metis.zoho.ZohoConstants;
import eu.europeana.metis.zoho.ZohoUtils;

public class UpdateOperation extends BaseOperation implements  Operation {

  private Record zohoOrganization;
  private OrganizationEnrichmentEntity organizationEnrichmentEntity;

  @Override
  public Date getModified() {
    return new Date(getZohoOrganization().getModifiedTime().toEpochSecond());
  }

  @Override
  public OrganizationEnrichmentEntity getOrganisationEnrichmentEntity() {
    return organizationEnrichmentEntity;
  }

  @Override
  public void setOrganisationEnrichmentEntity(OrganizationEnrichmentEntity organizationEnrichmentEntity) {
    this.organizationEnrichmentEntity = organizationEnrichmentEntity;
  }

  public UpdateOperation(Record org) {
    this.zohoOrganization = org;
    setAction(Constants.ACTION_UPDATE);
  }

  @Override
  public String getZohoId() {
    return Long.toString(getZohoOrganization().getId());
  }

  @Override
  public String getOrganizationId() {
    return getOrganisationEnrichmentEntity().getAbout();
  }

  @Override
  public Record getZohoOrganization() {
    return zohoOrganization;
  }

  public void setZohoOrganization(Record zohoOrganization) {
    this.zohoOrganization = zohoOrganization;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof UpdateOperation)
      return false;

    UpdateOperation op2 = (UpdateOperation) obj;
    return getModified().equals(op2.getModified()) && getZohoId().equals(op2.getZohoId())
        && getAction().equals(op2.getAction());
  }

  @Override
  public String toString() {
    return super.toString() + "; Acronym: " + ZohoUtils.stringFieldSupplier(getZohoOrganization().getKeyValue(ZohoConstants.ACRONYM_FIELD));
  }
}
