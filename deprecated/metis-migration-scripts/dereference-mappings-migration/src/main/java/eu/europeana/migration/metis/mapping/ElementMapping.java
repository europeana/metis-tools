package eu.europeana.migration.metis.mapping;

import eu.europeana.migration.metis.utils.ObjectIdentityUtils;

/**
 * This class represents a mapping from one element to another. Equality is defined as both source
 * and target elements being equal.
 * 
 * @author jochen
 *
 */
public class ElementMapping {

  private final Element from;
  private final Element to;

  /**
   * Constructor.
   * 
   * @param from The source element from which this mapping maps.
   * @param to The target element to which this mapping maps.
   */
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

  /**
   * 
   * @return The source element from which this mapping maps.
   */
  public Element getFrom() {
    return from;
  }

  /**
   * 
   * @return The target element to which this mapping maps.
   */
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

  /**
   * Will give a string representation of the source and the target separated by an arrow.
   */
  @Override
  public String toString() {
    return from.toString() + " -> " + to.toString();
  }
}
