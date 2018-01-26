package eu.europeana.migration.metis.mapping;

import eu.europeana.metis.dereference.ContextualClass;
import eu.europeana.migration.metis.utils.Namespace;

public enum Type {

  AGENT(new Element(Namespace.EDM, "agent"), "ag_"),

  TIMESPAN(new Element(Namespace.EDM, "timespan"), "ts_"),

  PLACE(new Element(Namespace.EDM, "place"), "pl_"),

  CONCEPT(new Element(Namespace.SKOS, "concept"), "cc_");

  private static final String PLACEHOLDER_SEPARATOR = "_";

  private final Element mainTag;
  private final String tagPrefix;

  private Type(Element mainTag, String tagPrefix) {
    this.mainTag = mainTag;
    this.tagPrefix = tagPrefix;
  }

  public Element getMainTag() {
    return mainTag;
  }

  public String getTagPrefix() {
    return tagPrefix;
  }

  public static Type getTypeForTag(String tagPlaceholder) {
    for (Type type : values()) {
      if (type.mainTag.toString(PLACEHOLDER_SEPARATOR).equals(tagPlaceholder)
          || tagPlaceholder.startsWith(type.tagPrefix)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Could not determine the type of tag: " + tagPlaceholder);
  }

  public static ElementWithType getTag(String tagPlaceholder,
      NamespaceCollection namespaceCollection) {
    final Type type = getTypeForTag(tagPlaceholder);
    final String tagName;
    if (type.mainTag.toString(PLACEHOLDER_SEPARATOR).equals(tagPlaceholder)) {
      tagName = tagPlaceholder;
    } else {
      tagName = tagPlaceholder.substring(type.tagPrefix.length());
    }
    final Element tag = Element.create(tagName, PLACEHOLDER_SEPARATOR, namespaceCollection);
    return new ElementWithType(tag, type);
  }

  public static class ElementWithType {

    private final Element element;
    private final Type type;

    private ElementWithType(Element element, Type type) {
      this.element = element;
      this.type = type;
    }

    public Element getElement() {
      return element;
    }

    public Type getType() {
      return type;
    }
  }

  public static ContextualClass convertToMetisType(Type type) {
    final ContextualClass result;
    switch (type) {
      case AGENT:
        result = ContextualClass.AGENT;
        break;
      case CONCEPT:
        result = ContextualClass.CONCEPT;
        break;
      case PLACE:
        result = ContextualClass.PLACE;
        break;
      case TIMESPAN:
        result = ContextualClass.TIMESPAN;
        break;
      default:
        throw new IllegalStateException("Cannot happen! Received type " + type.name());
    }
    return result;
  }
}
