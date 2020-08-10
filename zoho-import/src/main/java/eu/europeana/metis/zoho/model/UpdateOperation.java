package eu.europeana.metis.zoho.model;

import java.util.Date;
import eu.europeana.corelib.solr.entity.Organization;
import eu.europeana.enrichment.api.external.model.zoho.ZohoOrganization;

public class UpdateOperation extends BaseOperation implements  Operation {

  private ZohoOrganization zohoOrganization;
  private Organization edmOrganization;

  @Override
  public Date getModified() {
    return getZohoOrganization().getModified();
  }

  public UpdateOperation(ZohoOrganization org) {
    this.zohoOrganization = org;
    setAction(ACTION_UPDATE); 
  }

  @Override
  public String getZohoId() {
    return getZohoOrganization().getZohoId();
  }

  @Override
  public String getEdmOrganizationId() {
    return getEdmOrganization().getAbout();
  }
  
  public ZohoOrganization getZohoOrganization() {
    return zohoOrganization;
  }

  public void setZohoOrganization(ZohoOrganization zohoOrganization) {
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
  public Organization getEdmOrganization() {
    return edmOrganization;
  }

  public void setEdmOrganization(Organization edmOrganization) {
    this.edmOrganization = edmOrganization;
  }
  
  @Override
  public String toString() {
    return super.toString() + "; Acronym: " + getZohoOrganization().getAcronym();
  }
}
