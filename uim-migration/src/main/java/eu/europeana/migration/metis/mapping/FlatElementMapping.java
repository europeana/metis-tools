package eu.europeana.migration.metis.mapping;

import eu.europeana.migration.metis.mapping.Type.ElementWithType;
import eu.europeana.migration.metis.utils.MigrationUtils;
import eu.europeana.migration.metis.utils.Namespace;

class FlatElementMapping {

  private static final String PREFIX_SEPARATOR = ":";
  private static final String FROM_ELEMENT_SEPARATOR = "_";

  private final Element fromTag;
  private final Element fromAttribute;
  private final Element toTag;
  private final Element toAttribute;
  private final Type type;

  private FlatElementMapping(Element fromTag, Element fromAttribute, Element toTag, Element toAttribute,
      Type type) {
    if (fromTag == null) {
      throw new IllegalArgumentException("From tag cannot be null.");
    }
    if (toTag == null) {
      throw new IllegalArgumentException("To tag cannot be null.");
    }
    this.fromTag = fromTag;
    this.fromAttribute = fromAttribute;
    this.toTag = toTag;
    this.toAttribute = toAttribute;
    this.type = type;
  }

  protected static FlatElementMapping create(NamespaceCollection inputNamespaces, String from,
      String toLabel, String toAttributeString) {

    // Analyze the from string: get the from tag.
    final Namespace tagNamespace = inputNamespaces.startsWithKnownPrefix(from, PREFIX_SEPARATOR);
    final int tagNameStart = tagNamespace.getPrefix().length() + PREFIX_SEPARATOR.length();
    final int splitIndex = from.indexOf(FROM_ELEMENT_SEPARATOR, tagNameStart);
    final int tagNameEnd = splitIndex < 0 ? from.length() : splitIndex;
    final String tagName = from.substring(tagNameStart, tagNameEnd);
    final Element fromTag = new Element(tagNamespace, tagName);

    // Analyze the from string: get the from attribute.
    final Element fromAttribute;
    if (splitIndex < 0) {
      fromAttribute = null;
    } else {
      final String fromAttrString = from.substring(tagNameEnd + FROM_ELEMENT_SEPARATOR.length());
      fromAttribute = Element.create(fromAttrString, PREFIX_SEPARATOR, inputNamespaces);
    }

    // Create to elements
    final ElementWithType toTag = Type.getTag(toLabel, NamespaceSet.OUTPUT_COLLECTION);
    final Element toAttribute;
    if (toAttributeString == null || toAttributeString.trim().isEmpty()) {
      toAttribute = null;
    } else {
      toAttribute = Element.create(toAttributeString.trim(), PREFIX_SEPARATOR,
          NamespaceSet.OUTPUT_COLLECTION);
    }

    // Create mapping
    return new FlatElementMapping(fromTag, fromAttribute, toTag.getElement(), toAttribute,
        toTag.getType());
  }

  public Element getFromTag() {
    return fromTag;
  }

  public Element getFromAttribute() {
    return fromAttribute;
  }

  public Element getToTag() {
    return toTag;
  }

  public Element getToAttribute() {
    return toAttribute;
  }

  public Type getType() {
    return type;
  }

  @Override
  public boolean equals(Object otherObject) {
    if (!(otherObject instanceof FlatElementMapping)) {
      return false;
    }
    final FlatElementMapping other = (FlatElementMapping) otherObject;
    return getFromTag().equals(other.getFromTag())
        && MigrationUtils.equalsIncludingNull(getFromAttribute(), other.getFromAttribute())
        && getToTag().equals(other.getToTag())
        && MigrationUtils.equalsIncludingNull(getToAttribute(), other.getToAttribute());
  }

  @Override
  public int hashCode() {
    return MigrationUtils.hashCodeOfMultipleNullableObjects(getFromTag(), getFromAttribute(),
        getToTag(), getToAttribute());
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    result.append(fromTag.toString());
    if (fromAttribute != null) {
      result.append("@").append(fromAttribute.toString());
    }
    result.append(" -> ").append(toTag.toString());
    if (toAttribute != null) {
      result.append("@").append(toAttribute.toString());
    }
    return result.toString();
  }
}


