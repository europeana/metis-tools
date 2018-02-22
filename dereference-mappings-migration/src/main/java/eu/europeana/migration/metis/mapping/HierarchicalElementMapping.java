package eu.europeana.migration.metis.mapping;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class represents an XSL element mapping (tag plus all its attributes) that is to be used in
 * a larger data structure (hierarchical in the sense that this class in combination with this
 * datastructure define relationships between the different mappings). Equality is defined as the
 * tag mapping being equal, so that we can make sets of these mappings. For this we don't need to
 * consider the attribute mappings.
 * 
 * @author jochen
 *
 */
public class HierarchicalElementMapping {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private final boolean includeValueOfTag;
  private final ElementMapping tagMapping;
  private final Set<ElementMapping> attributeMappings;

  /**
   * Constructor.
   * 
   * @param includeValueOfTag Whether the textual content of the tag is to be copied along with the
   *        tag.
   * @param tagMapping The mapping from a source tag to a target tag.
   * @param attributeMappings The mappings from attributes of the source tag to attributes of the
   *        target tag.
   */
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

  /**
   * 
   * @return The attribute mappings (mappings from attributes in the source tag to attributes in the
   *         target tag).
   */
  public Set<ElementMapping> getAttributeMappings() {
    return attributeMappings;
  }

  /**
   * 
   * @return The tag mapping (from source tag to target tag).
   */
  public ElementMapping getTagMapping() {
    return tagMapping;
  }

  /**
   * 
   * @return Whether the textual content of the tag is to be copied along with the tag.
   */
  public boolean includeValueOfTag() {
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

  /**
   * Will return the tag mapping followed by a list of the attribute mappings. Will also indicate if
   * the textual value of the tag is excluded from the mapping.
   */
  @Override
  public String toString() {
    final String attributeSeparator = LINE_SEPARATOR + "-- ";
    final String attributeMappingString = attributeMappings.isEmpty() ? ""
        : (attributeSeparator + toString(attributeMappings, attributeSeparator));
    return tagMapping.toString() + (includeValueOfTag ? "" : " [Tag contents not included]")
        + attributeMappingString;
  }
}
