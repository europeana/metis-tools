package eu.europeana.migration.metis.mapping;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class HierarchicalElementMapping {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private final boolean includeValueOfTag;
  private final ElementMapping tagMapping;
  private final Set<ElementMapping> attributeMappings;

  HierarchicalElementMapping(boolean includeValueOfTag, ElementMapping tagMapping,
      Set<ElementMapping> attributeMappings) {
    if (tagMapping == null) {
      throw new IllegalArgumentException("Tag mappging cannot be null.");
    }
    if (attributeMappings == null) {
      throw new IllegalArgumentException("Attribute mapping set cannot be null.");
    }
    this.includeValueOfTag = includeValueOfTag;
    this.tagMapping = tagMapping;
    this.attributeMappings = Collections.unmodifiableSet(attributeMappings);
  }

  public Set<ElementMapping> getAttributeMappings() {
    return attributeMappings;
  }

  public ElementMapping getTagMapping() {
    return tagMapping;
  }

  public boolean isIncludeValueOfTag() {
    return includeValueOfTag;
  }

  @Override
  public boolean equals(Object otherObject) {
    if (!(otherObject instanceof HierarchicalElementMapping)) {
      return false;
    }
    return getTagMapping().equals(((HierarchicalElementMapping) otherObject).getTagMapping());
  }

  @Override
  public int hashCode() {
    return getTagMapping().hashCode();
  }

  private static String toString(Set<ElementMapping> mappings, String separator) {
    return mappings.stream().map(ElementMapping::toString).collect(Collectors.joining(separator));
  }

  @Override
  public String toString() {
    final String attributeSeparator = LINE_SEPARATOR + "-- ";
    final String attributeMappingString = attributeMappings.isEmpty() ? ""
        : (attributeSeparator + toString(attributeMappings, attributeSeparator));
    return tagMapping.toString() + (includeValueOfTag ? "" : " [Tag contents not included]")
        + attributeMappingString;
  }
}
