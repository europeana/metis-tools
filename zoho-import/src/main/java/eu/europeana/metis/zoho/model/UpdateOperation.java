package eu.europeana.metis.zoho.model;

import java.util.Date;

import com.zoho.crm.library.crud.ZCRMRecord;

import eu.europeana.corelib.definitions.edm.entity.Organization;
import eu.europeana.metis.zoho.DateUtils;
import eu.europeana.metis.zoho.ZohoConstants;

public class UpdateOperation extends BaseOperation implements  Operation {

  private ZCRMRecord zohoOrganization;
  private Organization edmOrganization;

  @Override
  public Date getModified() {
    return DateUtils.parseDate(getZohoOrganization().getModifiedTime());
  }

  public UpdateOperation(ZCRMRecord org) {
    this.zohoOrganization = org;
    setAction(ACTION_UPDATE); 
  }

  @Override
  public String getZohoId() {
    return getZohoOrganization().getEntityId().toString();
  }
  
  @Override
  public String getEdmOrganizationId() {
    return getEdmOrganization().getAbout();
  }
  
  public ZCRMRecord getZohoOrganization() {
    return zohoOrganization;
  }

  public void setZohoOrganization(ZCRMRecord zohoOrganization) {
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
    return super.toString() + "; Acronym: " + getZohoOrganization().getData().get(ZohoConstants.ACRONYM_FIELD);
  }
}
