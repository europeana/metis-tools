package eu.europeana.corelib.dereference.impl;

import java.io.Serializable;

public class EdmMappedField implements Serializable {

  private static final long serialVersionUID = 1L;

  private String label;

  private String attribute;

  public EdmMappedField() {}

  public EdmMappedField(String label, String attribute) {
    this.label = label;
    this.attribute = attribute;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public String getAttribute() {
    return attribute;
  }

  public void setAttribute(String attribute) {
    this.attribute = attribute;
  }
}
