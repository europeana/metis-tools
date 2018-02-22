package eu.europeana.migration.metis.mapping;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import eu.europeana.corelib.dereference.impl.ControlledVocabularyImpl;
import eu.europeana.corelib.dereference.impl.EdmMappedField;
import eu.europeana.migration.metis.utils.LogUtils;
import eu.europeana.migration.metis.utils.Namespace;

/**
 * <p>
 * This class represents a collection of all element mappings in a vocabulary. It consists of a
 * parent mapping (a unique mapping to the main tag associated with the {@link Type} - see
 * {@link Type#getMainTag()}) and a list of child mappings.
 * </p>
 * <p>
 * All mappings include a tag mapping and a collection of attribute mappings and are required to be
 * of the same {@link Type}. Furthermore, the parent mapping must contain exactly one document ID
 * mapping (an attribute mapping that maps the document ID which is a mapping to the
 * {@value #DOCUMENT_ID_ATTRIBUTE} attribute of the parent target tag).
 * </p>
 * 
 * @author jochen
 *
 */
public class ElementMappings {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private static final Element[] IGNORED_TAG_ARRAY =
      new Element[] {new Element(Namespace.RDF, "type"), new Element(Namespace.RDF, "RDF")};
  private static final Set<Element> IGNORED_TAG_SET =
      Arrays.stream(IGNORED_TAG_ARRAY).collect(Collectors.toSet());

  private static final Element DOCUMENT_ID_ATTRIBUTE = new Element(Namespace.RDF, "about");

  private final HierarchicalElementMapping parentMapping;
  private final Set<HierarchicalElementMapping> childMappings;
  private final Type type;

  private ElementMappings(HierarchicalElementMapping parentMapping,
      Set<HierarchicalElementMapping> childMappings, Type type) {
    this.type = type;
    this.parentMapping = parentMapping;
    this.childMappings = Collections.unmodifiableSet(childMappings);
  }

  /**
   * This method creates an instance of this class for a UIM vocabulary. For the string
   * representations that are expected of the various elements see
   * {@link FlatElementMapping#create(NamespaceCollection, String, String, String)}. This method
   * will throw an exception if they are not of the same {@link Type} or if the parent mapping could
   * not be determined or if the document ID mapping could not be determined.
   * 
   * @param vocabulary the UIM vocabulary from which to extract the mappings.
   * @return The mappings.
   */
  public static ElementMappings create(ControlledVocabularyImpl vocabulary) {

    // Find the namespaces that are to be used.
    final NamespaceCollection namespaceCollection =
        NamespaceSet.getCollectionForInput(vocabulary.getURI());

    // Convert the mappings
    final Set<FlatElementMapping> flatMappingSet = new HashSet<>();
    for (Entry<String, List<EdmMappedField>> mapping : vocabulary.getElements().entrySet()) {
      if (mapping.getValue() == null) {
        continue;
      }
      for (EdmMappedField target : mapping.getValue()) {
        flatMappingSet.add(FlatElementMapping.create(namespaceCollection, mapping.getKey(),
            target.getLabel(), target.getAttribute()));
      }
    }

    // Check type
    final List<Type> types = flatMappingSet.stream().map(FlatElementMapping::getType).distinct()
        .collect(Collectors.toList());
    if (types.size() > 1) {
      throw new IllegalArgumentException("Found more than one different mapping type: "
          + types.stream().map(Type::name).collect(Collectors.joining(", ")));
    }
    final Type type = types.get(0);

    // Split into tag mappings and mappings involving attributes. Check that attribute mappings
    // always have a from attribute. We copy the content for all tag mappings (except possibly the
    // parent mapping, but this will be sorted out later).
    final Map<ElementMapping, Set<ElementMapping>> tagsWithContent = new HashMap<>();
    final Set<FlatElementMapping> attributeMappings = new HashSet<>();
    for (FlatElementMapping mapping : flatMappingSet) {
      final boolean isTagMapping =
          mapping.getFromAttribute() == null && mapping.getToAttribute() == null;
      if (isTagMapping) {
        tagsWithContent.put(new ElementMapping(mapping.getFromTag(), mapping.getToTag()),
            new HashSet<>());
      } else if (mapping.getFromAttribute() == null) {
        throw new IllegalArgumentException(
            "Attribute mapping " + mapping + " is missing a from attribute.");
      } else {
        attributeMappings.add(mapping);
      }
    }

    // Go by all attribute mappings and add it to a matching tag mapping if one exists, otherwise
    // create a new tag mapping (that does not copy content). If an attribute mapping does not
    // contain a to attribute, assume it is the same as the from attribute.
    final Map<ElementMapping, Set<ElementMapping>> tagsWithoutContent = new HashMap<>();
    for (FlatElementMapping mapping : attributeMappings) {
      final ElementMapping tagMapping =
          new ElementMapping(mapping.getFromTag(), mapping.getToTag());
      final ElementMapping normalizedMapping = new ElementMapping(mapping.getFromAttribute(),
          mapping.getToAttribute() == null ? mapping.getFromAttribute() : mapping.getToAttribute());
      if (tagsWithContent.containsKey(tagMapping)) {
        tagsWithContent.get(tagMapping).add(normalizedMapping);
      } else {
        if (!tagsWithoutContent.containsKey(tagMapping)) {
          tagsWithoutContent.put(tagMapping, new HashSet<>());
        }
        tagsWithoutContent.get(tagMapping).add(normalizedMapping);
      }
    }

    // Convert to hierarchical structure: find the parent mapping and remove the mappings that are
    // to be ignored. Check the parent mapping.
    HierarchicalElementMapping[] parentMapping = new HierarchicalElementMapping[1];
    final Set<HierarchicalElementMapping> childMappings = new HashSet<>();
    addHierarchicalMappings(type, false, tagsWithoutContent, childMappings, parentMapping);
    addHierarchicalMappings(type, true, tagsWithContent, childMappings, parentMapping);
    if (parentMapping[0] == null) {
      throw new IllegalArgumentException(
          "There is no mapping to the parent tag: " + type.getMainTag().toString() + ".");
    }
    if (getDocumentIdMappings(parentMapping[0]).size() != 1) {
      throw new IllegalArgumentException("There must be exactly one mapping to the attribute '"
          + DOCUMENT_ID_ATTRIBUTE.toString() + "' of the parent tag.");
    }

    // Done
    return new ElementMappings(parentMapping[0], childMappings, type);
  }

  private static void addHierarchicalMappings(Type type, boolean includeTagContent,
      Map<ElementMapping, Set<ElementMapping>> mappings,
      Set<HierarchicalElementMapping> childMappings, HierarchicalElementMapping[] parentMapping) {
    for (Entry<ElementMapping, Set<ElementMapping>> entry : mappings.entrySet()) {
      if (IGNORED_TAG_SET.contains(entry.getKey().getFrom())) {
        logIgnoredMapping(entry, includeTagContent);
        continue;
      }
      if (type.getMainTag().equals(entry.getKey().getTo())) {
        if (parentMapping[0] == null) {
          parentMapping[0] =
              new HierarchicalElementMapping(false, entry.getKey(), entry.getValue());
        } else {
          throw new IllegalArgumentException("Two mappings map to the parent tag: "
              + parentMapping[0].getTagMapping().toString() + " and " + entry.getKey() + ".");
        }
      } else {
        childMappings.add(
            new HierarchicalElementMapping(includeTagContent, entry.getKey(), entry.getValue()));
      }
    }
  }

  private static void logIgnoredMapping(Entry<ElementMapping, Set<ElementMapping>> entry,
      boolean includeTagContent) {
    final StringBuilder ignoreMessage = new StringBuilder();
    ignoreMessage.append("IGNORING FOUND MAPPING: ").append(entry.getKey());
    if (includeTagContent) {
      ignoreMessage.append(" [tag content ignored]");
    }
    if (!entry.getValue().isEmpty()) {
      ignoreMessage.append(" including attribute mappings: ");
      ignoreMessage.append(entry.getValue().stream().map(ElementMapping::toString)
          .collect(Collectors.joining(", ")));
    }
    LogUtils.logInfoMessage(ignoreMessage.toString());
  }

  /**
   * 
   * @return The type of this mapping.
   */
  public Type getType() {
    return type;
  }

  /**
   * 
   * @return The parent mapping.
   */
  public HierarchicalElementMapping getParentMapping() {
    return parentMapping;
  }

  /**
   * 
   * @return The child mappings.
   */
  public Set<HierarchicalElementMapping> getChildMappings() {
    return childMappings;
  }

  private static List<Element> getDocumentIdMappings(HierarchicalElementMapping parentMapping) {
    return parentMapping.getAttributeMappings().stream()
        .filter(mapping -> DOCUMENT_ID_ATTRIBUTE.equals(mapping.getTo()))
        .map(ElementMapping::getFrom).collect(Collectors.toList());
  }

  /**
   * 
   * @return The element mapping the document ID (the source attribute of the document ID mapping).
   */
  public Element getDocumentIdMapping() {
    return getDocumentIdMappings(getParentMapping()).get(0);
  }

  /**
   * Will return a representation of the parent mapping followed by a list of child mappings.
   */
  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    result.append("PARENT MAPPING:").append(LINE_SEPARATOR).append(parentMapping.toString());
    result.append(LINE_SEPARATOR).append("CHILD MAPPINGS:").append(LINE_SEPARATOR);
    result.append(childMappings.stream().map(HierarchicalElementMapping::toString)
        .collect(Collectors.joining(LINE_SEPARATOR)));
    return result.toString();
  }
}
