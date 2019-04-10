package eu.europeana.metis.zoho.model;

import java.util.Date;

import com.zoho.crm.library.crud.ZCRMRecord;

import eu.europeana.corelib.definitions.edm.entity.Organization;

public interface Operation extends Comparable<Operation>{

  public static final String ACTION_CREATE = "create";
  public static final String ACTION_UPDATE = "update";
  public static final String ACTION_DELETE = "delete";
  public static final String URL_ORGANIZATION_PREFFIX = "http://data.europeana.eu/organization/";
  public static final String SEMICOLON = ";";
  
  String getEdmOrganizationId();

  String getZohoId();

  String getAction();

  Date getModified();

  Organization getEdmOrganization();

  ZCRMRecord getZohoOrganization();
  
  void setEdmOrganization(Organization edmOrganization);
}
