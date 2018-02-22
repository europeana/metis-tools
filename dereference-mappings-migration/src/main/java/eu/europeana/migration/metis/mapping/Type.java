package eu.europeana.migration.metis.mapping;

import eu.europeana.metis.dereference.ContextualClass;
import eu.europeana.migration.metis.utils.Namespace;

/**
 * Instances of this class represent an output type, as defined by the root node to be generated.
 * 
 * @author jochen
 *
 */
public enum Type {

  /** The agent type. **/
  AGENT(new Element(Namespace.EDM, "agent"), "ag_", ContextualClass.AGENT),

  /** The timespan type. **/
  TIMESPAN(new Element(Namespace.EDM, "timespan"), "ts_", ContextualClass.TIMESPAN),

  /** The place type. **/
  PLACE(new Element(Namespace.EDM, "place"), "pl_", ContextualClass.PLACE),

  /** The concept type. **/
  CONCEPT(new Element(Namespace.SKOS, "concept"), "cc_", ContextualClass.CONCEPT);

  private static final String PLACEHOLDER_SEPARATOR = "_";

  private final Element mainTag;
  private final String typePrefix;
  private final ContextualClass metisType;

  private Type(Element mainTag, String typePrefix, ContextualClass metisType) {
    this.mainTag = mainTag;
    this.typePrefix = typePrefix;
    this.metisType = metisType;
  }

  /**
   * 
   * @return The type's main (root) tag: the tag that is the parent tag for all content of the given
   *         type.
   */
  public Element getMainTag() {
    return mainTag;
  }

  /**
   * 
   * @return The prefix associated with this type as known to UIM (i.e. the prefix that comes before
   *         all target tags of this type except the root type and by which UIM distinguishes to
   *         which type the target tag belongs).
   */
  public String getTypePrefix() {
    return typePrefix;
  }

  /**
   * 
   * @return The Metis type associated with this type.
   */
  public ContextualClass getMetisType() {
    return metisType;
  }

  /**
   * <p>
   * Determine the type based on the target tag string representation from UIM. This method looks at
   * the prefix (see {@link #getTypePrefix()}). Note that this method expects a placeholder of the
   * form:
   * </p>
   * <p>
   * {type prefix}{namespace prefix}{@value #PLACEHOLDER_SEPARATOR}{tag name}
   * </p>
   * 
   * @param tagPlaceholder
   * @return the type associated with the placeholder.
   */
  public static Type getTypeForTag(String tagPlaceholder) {
    for (Type type : values()) {
      if (type.mainTag.toString(PLACEHOLDER_SEPARATOR).equals(tagPlaceholder)
          || tagPlaceholder.startsWith(type.typePrefix)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Could not determine the type of tag: " + tagPlaceholder);
  }

  /**
   * Converts the target tag placeholder to an element object (with a type).
   * 
   * @param tagPlaceholder The target tag placeholder (see {@link #getTypeForTag(String)}).
   * @param namespaceCollection The collection of namespaces that are known in the target data.
   * @return The element with the type (see {@link #getTypeForTag(String)}).
   */
  public static ElementWithType getTag(String tagPlaceholder,
      NamespaceCollection namespaceCollection) {
    final Type type = getTypeForTag(tagPlaceholder);
    final String tagName;
    if (type.mainTag.toString(PLACEHOLDER_SEPARATOR).equals(tagPlaceholder)) {
      tagName = tagPlaceholder;
    } else {
      tagName = tagPlaceholder.substring(type.typePrefix.length());
    }
    final Element tag = Element.create(tagName, PLACEHOLDER_SEPARATOR, namespaceCollection);
    return new ElementWithType(tag, type);
  }

  /**
   * Tuple combining an {@link Element} and a {@link Type}.
   * 
   * @author jochen
   *
   */
  public static class ElementWithType {

    private final Element element;
    private final Type type;

    private ElementWithType(Element element, Type type) {
      this.element = element;
      this.type = type;
    }

    /**
     * 
     * @return The element.
     */
    public Element getElement() {
      return element;
    }

    /**
     * 
     * @return The type.
     */
    public Type getType() {
      return type;
    }
  }
}
