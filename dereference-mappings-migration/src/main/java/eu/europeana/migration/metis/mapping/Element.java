package eu.europeana.migration.metis.mapping;

import eu.europeana.migration.metis.utils.ObjectIdentityUtils;
import eu.europeana.migration.metis.utils.Namespace;

/**
 * <p>
 * This class represents an XML element (tag or attribute), consisting of a name and a namespace.
 * </p>
 * <p>
 * Equality is defined based on the element name in combination with the namespace uri (so not the
 * namespace prefix, as this can change).
 * </p>
 * 
 * @author jochen
 *
 */
public final class Element {

  private static final String TAG_SEPARATOR = ":";

  private final Namespace namespace;
  private final String tagName;

  /**
   * Constructor.
   * 
   * @param namespace The namespace.
   * @param tagName The element name.
   */
  public Element(Namespace namespace, String tagName) {
    if (namespace == null) {
      throw new IllegalArgumentException("Namespace cannot be null");
    }
    if (tagName == null || tagName.trim().isEmpty()) {
      throw new IllegalArgumentException("Tag name cannot be null or empty:" + tagName);
    }
    this.namespace = namespace;
    this.tagName = tagName.trim();
  }

  /**
   * Method for creating an element based on an input string and a namespace collection to choose
   * the namespace from. This method will throw an exception if the namespace prefix in the input
   * string does not match any of the namespaces in the namespace collection.
   * 
   * @param input The input string.
   * @param separator The separator expected between the namespace prefix and the element name.
   * @param namespaceCollection The namespace collection to which the namespace of the element is
   *        limited.
   * @return The element.
   */
  protected static Element create(String input, String separator,
      NamespaceCollection namespaceCollection) {
    final Namespace namespace = namespaceCollection.getNamespaceForTag(input, separator);
    final String name = input.substring(namespace.getPrefix().length() + separator.length());
    return new Element(namespace, name);
  }

  /**
   * 
   * @return The tag name of the element.
   */
  public String getTagName() {
    return tagName;
  }

  /**
   * 
   * @return The namespace of the element.
   */
  public Namespace getNamespace() {
    return namespace;
  }

  @Override
  public boolean equals(Object otherObject) {
    if (!(otherObject instanceof Element)) {
      return false;
    }
    final Element other = (Element) otherObject;
    return getNamespace().getUri().equals(other.getNamespace().getUri())
        && getTagName().equals(other.getTagName());
  }

  @Override
  public int hashCode() {
    return ObjectIdentityUtils.hashCodeOfMultipleNullableObjects(getNamespace().getUri(),
        getTagName());
  }

  /**
   * Is a wrapper for {@link #toString(String)} with separator {@value #TAG_SEPARATOR}.
   */
  @Override
  public String toString() {
    return toString(TAG_SEPARATOR);
  }

  /**
   * <p>
   * Returns a string representation of this element. The representation will be of the form:
   * </p>
   * <p>
   * {namespace prefix}{separator}{element name}
   * </p>
   * 
   * @param separator the separator to use in the string representation.
   * @return The string representation.
   */
  public String toString(String separator) {
    return namespace.getPrefix() + separator + tagName;
  }
}
