package eu.europeana.metis.zoho.model;

import java.util.Date;
import eu.europeana.corelib.solr.entity.Organization;
import eu.europeana.enrichment.api.external.model.zoho.ZohoOrganization;
import eu.europeana.enrichment.service.zoho.ZohoAccessService;

public class DeleteOperation extends BaseOperation{

   String zohoId;
   String edmOrganizationId;
   Date modified;
  
  public DeleteOperation(String zohoId, Date modified){
    this.zohoId=zohoId;
    this.modified = modified;
    this.edmOrganizationId = ZohoAccessService.URL_ORGANIZATION_PREFFIX + zohoId;
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
  public ZohoOrganization getZohoOrganization() {
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
