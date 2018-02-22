package eu.europeana.migration.metis.mapping;

import eu.europeana.migration.metis.utils.ObjectIdentityUtils;

public class ElementMapping {

  private final Element from;
  private final Element to;

  ElementMapping(Element from, Element to) {
    if (from == null) {
      throw new IllegalArgumentException("From tag cannot be null.");
    }
    if (to == null) {
      throw new IllegalArgumentException("To tag cannot be null.");
    }
    this.from = from;
    this.to = to;
  }

  public Element getFrom() {
    return from;
  }

  public Element getTo() {
    return to;
  }

  @Override
  public boolean equals(Object otherObject) {
    if (!(otherObject instanceof ElementMapping)) {
      return false;
    }
    final ElementMapping other = (ElementMapping) otherObject;
    return getFrom().equals(other.getFrom()) && getTo().equals(other.getTo());
  }

  @Override
  public int hashCode() {
    return ObjectIdentityUtils.hashCodeOfMultipleNullableObjects(getFrom(), getTo());
  }

  @Override
  public String toString() {
    return from.toString() + " -> " + to.toString();
  }

  /**
   * Determine whether this mapping maps a tag to itself.
   * 
   * @return
   */
  boolean mapsToSameTag() {
    return getFrom().equals(getTo());
  }
}
