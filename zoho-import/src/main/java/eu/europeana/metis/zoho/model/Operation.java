package eu.europeana.metis.zoho.model;

import java.util.Date;
import eu.europeana.corelib.solr.entity.Organization;
import eu.europeana.enrichment.api.external.model.zoho.ZohoOrganization;

public interface Operation extends Comparable<Operation>{

  public static final String ACTION_CREATE = "create";
  public static final String ACTION_UPDATE = "update";
  public static final String ACTION_DELETE = "delete";
  
  String getEdmOrganizationId();

  String getZohoId();

  String getAction();

  Date getModified();

  Organization getEdmOrganization();

  ZohoOrganization getZohoOrganization();
  
  void setEdmOrganization(Organization edmOrganization);
}
