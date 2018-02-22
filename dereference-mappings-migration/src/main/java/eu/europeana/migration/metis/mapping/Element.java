package eu.europeana.migration.metis.mapping;

import eu.europeana.migration.metis.utils.MigrationUtils;
import eu.europeana.migration.metis.utils.Namespace;

public final class Element {

  private static final String TAG_SEPARATOR = ":";

  private final Namespace namespace;
  private final String tagName;

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

  protected static Element create(String input, String separator,
      NamespaceCollection namespaceCollection) {
    final Namespace namespace = namespaceCollection.startsWithKnownPrefix(input, separator);
    final String name = input.substring(namespace.getPrefix().length() + separator.length());
    return new Element(namespace, name);
  }

  public String getTagName() {
    return tagName;
  }

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
    return MigrationUtils.hashCodeOfMultipleNullableObjects(getNamespace().getUri(), getTagName());
  }

  @Override
  public String toString() {
    return toString(TAG_SEPARATOR);
  }

  public String toString(String separator) {
    return namespace.getPrefix() + separator + tagName;
  }
}
