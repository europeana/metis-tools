package eu.europeana.metis.zoho.model;

import java.util.Date;

import com.zoho.crm.library.crud.ZCRMRecord;

import eu.europeana.corelib.definitions.edm.entity.Organization;

public class DeleteOperation extends BaseOperation{

   String zohoId;
   String edmOrganizationId;
   Date modified;
  
  public DeleteOperation(String zohoId, Date modified){
    this.zohoId=zohoId;
    this.modified = modified;
    this.edmOrganizationId = Operation.URL_ORGANIZATION_PREFFIX + zohoId;
    setAction(ACTION_DELETE);
  }

  public String getZohoId() {
    return zohoId;
  }

  @Override
  public String getEdmOrganizationId() {
    return edmOrganizationId;
  }

  @Override
  public Date getModified() {
    return modified;
  }

  @Override
  public ZCRMRecord getZohoOrganization() {
    // Not used in delete operations
    return null;
  }

  @Override
  public Organization getEdmOrganization() {
    // Not used in delete operations
    return null;
  }

  @Override
  public void setEdmOrganization(Organization edmOrganization) {
    // Not used in delete operations   
  }
}
