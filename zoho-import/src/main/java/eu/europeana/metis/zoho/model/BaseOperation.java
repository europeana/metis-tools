package eu.europeana.metis.zoho.model;

public abstract class BaseOperation implements Operation{

  private String action;
  
  @Override
  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }
  
  @Override
  public int hashCode() {
    return getModified().hashCode();
  }
  
  @Override
  public int compareTo(Operation o) {
    int ret = getModified().compareTo(o.getModified());
    if (ret == 0)
      ret = getZohoId().compareTo(o.getZohoId());
    return ret;
  }
  
  @Override
  public String toString() {
    String serialization = "Action :" + getAction() + "; Zoho id: " + getZohoId();
    if(getEdmOrganization() != null)
      serialization+= "; EdmOrganizationId: " + getEdmOrganizationId();
    return serialization;
  }
  
}
