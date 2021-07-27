package eu.europeana.metis.zoho.model;

public class ImportStatus {

  int importedMetis = 0;
  int deletedMetis = 0;
  int importedEntityApi = 0;
  int deletedEntityApi = 0;
  int notAvailable = 0;
  UpdateOperation failedOperation;

  public int getImportedMetis() {
    return importedMetis;
  }

  public int getDeletedMetis() {
    return deletedMetis;
  }

  public int getImportedEntityApi() {
    return importedEntityApi;
  }

	public int getNotAvailableInMetis() {
		return notAvailable;
	}
  
  public int getDeletedEntityApi() {
    return deletedEntityApi;
  }

  public void incrementImportedMetis() {
    importedMetis++;
  }

  public void incrementdeletedMetis() {
    deletedMetis++;
  }

  public void incrementdeletedMetis( int nr) {
    deletedMetis += nr ;
  }
  
  public void incrementImportedEntityApi() {
    importedEntityApi++;
  }

  public void incrementdeletedEntityApi() {
    deletedEntityApi++;
  }

  public void incrementNotAvailableInMetis() {
	  notAvailable++;
  }
  
  public UpdateOperation getFailedOperation() {
    return failedOperation;
  }

  public void setFailedOperation(UpdateOperation failedOperation) {
    this.failedOperation = failedOperation;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Imported in Metis: ").append(getImportedMetis())
        .append("; Imported in Entity Api: ").append(getImportedEntityApi())
        .append("; Deleted in Metis: ").append(getDeletedMetis())
        .append("; Not available in Metis (for deletion): ").append(getNotAvailableInMetis());
    if (getFailedOperation() != null)
      builder.append("\nFailed operation: ").append(getFailedOperation().toString());
    
    return builder.toString();
  }
}
