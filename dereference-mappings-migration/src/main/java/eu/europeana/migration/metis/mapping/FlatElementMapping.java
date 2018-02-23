package eu.europeana.migration.metis.mapping;

import eu.europeana.migration.metis.mapping.Type.ElementWithType;
import eu.europeana.migration.metis.utils.ObjectIdentityUtils;
import eu.europeana.migration.metis.utils.Namespace;

/**
 * This class represents an isolated element mapping (flat in the sense that it has no parents or
 * children nor is any other relationship between different instances of this class defined). The
 * source and target elements can be either a tag or a tag plus attribute. Equality is defined as
 * both source and target elements, both tag and attribute, being equal. We will consider two null
 * attributes to be equal.
 * 
 * @author jochen
 *
 */
class FlatElementMapping {

  private static final String PREFIX_SEPARATOR = ":";
  private static final String FROM_ELEMENT_SEPARATOR = "_";
  private static final String ATTRIBUTE_SEPARATOR = "@";

  private final Element fromTag;
  private final Element fromAttribute;
  private final Element toTag;
  private final Element toAttribute;
  private final Type type;

  private FlatElementMapping(Element fromTag, Element fromAttribute, Element toTag,
      Element toAttribute, Type type) {
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

  /**
   * <p>
   * Creates an instance of this class. The source element is expected to have the form:
   * </p>
   * <p>
   * {tag namespace prefix}{@value #PREFIX_SEPARATOR}{tag
   * name}({@value #FROM_ELEMENT_SEPARATOR}{attribute namespace
   * prefix}{@value #PREFIX_SEPARATOR}{attribute name})?
   * </p>
   * <p>
   * Where the attribute section (between parentheses) is optional. The target tag string is
   * expected to look like a placeholder in the sense of {@link Type#getTypeForTag(String)}. The
   * target attribute string is expected to have the form:
   * </p>
   * <p>
   * {attribute namespace prefix}{@value #PREFIX_SEPARATOR}{attribute name}
   * </p>
   * 
   * @param inputNamespaces The namespaces that are valid for the source element.
   * @param from A string representation of the source element (either tag or tag plus attribute).
   *        This cannot be null.
   * @param toLabel A string representation of the target tag. This cannot be null.
   * @param toAttributeString A string representation of the target attribute. This can be null if
   *        the target is not an attribute but a tag.
   * @return An instance of this class representing the given mapping.
   */
  protected static FlatElementMapping create(NamespaceCollection inputNamespaces, String from,
      String toLabel, String toAttributeString) {

    // Analyze the from string: get the from tag.
    final Namespace tagNamespace = inputNamespaces.getNamespaceForTag(from, PREFIX_SEPARATOR);
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

  /**
   * 
   * @return The source tag. Is not null.
   */
  public Element getFromTag() {
    return fromTag;
  }

  /**
   * 
   * @return The source attribute. Is null if the source is not an attribute.
   */
  public Element getFromAttribute() {
    return fromAttribute;
  }

  /**
   * 
   * @return The target tag. Is not null.
   */
  public Element getToTag() {
    return toTag;
  }

  /**
   * 
   * @return The target attribute. Is null if the target is not an attribute.
   */
  public Element getToAttribute() {
    return toAttribute;
  }

  /**
   * 
   * @return The type of the mapping (i.e. the type of the target tag).
   */
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
        && ObjectIdentityUtils.equalsIncludingNull(getFromAttribute(), other.getFromAttribute())
        && getToTag().equals(other.getToTag())
        && ObjectIdentityUtils.equalsIncludingNull(getToAttribute(), other.getToAttribute());
  }

  @Override
  public int hashCode() {
    return ObjectIdentityUtils.hashCodeOfMultipleNullableObjects(getFromTag(), getFromAttribute(),
        getToTag(), getToAttribute());
  }

  /**
   * Will return a string representation of the source element (tag plus attribute) and of the
   * target element separated by an arrow. Tag and attribute are separated by
   * {@value #ATTRIBUTE_SEPARATOR}.
   */
  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    result.append(fromTag.toString());
    if (fromAttribute != null) {
      result.append(ATTRIBUTE_SEPARATOR).append(fromAttribute.toString());
    }
    result.append(" -> ").append(toTag.toString());
    if (toAttribute != null) {
      result.append(ATTRIBUTE_SEPARATOR).append(toAttribute.toString());
    }
    return result.toString();
  }
}


