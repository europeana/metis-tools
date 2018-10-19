package eu.europeana.metis.zoho.exception;

import eu.europeana.metis.zoho.model.Operation;

public class OrganizationImportException extends Exception {

  private static final long serialVersionUID = -4810544920106125669L;
  private Operation operation;
  private String processingStep;

  public OrganizationImportException(Operation operation, String processingStep, Throwable th) {
    super("Import operation failed! Cound not perform action " + processingStep
        + " when performing operation: " + operation, th);
    this.operation = operation;
    this.processingStep = processingStep;
  }

  public Operation getOperation() {
    return operation;
  }

  public String getProcessingStep() {
    return processingStep;
  }

}
