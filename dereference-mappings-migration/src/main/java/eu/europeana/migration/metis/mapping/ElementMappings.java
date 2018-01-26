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

public class ElementMappings {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private static final Element[] IGNORED_TAG_ARRAY =
      new Element[] {new Element(Namespace.RDF, "type"), new Element(Namespace.RDF, "RDF")};
  private static final Set<Element> IGNORED_TAG_SET =
      Arrays.stream(IGNORED_TAG_ARRAY).collect(Collectors.toSet());

  private final HierarchicalElementMapping parentMapping;
  private final Set<HierarchicalElementMapping> childMappings;
  private final Type type;

  private ElementMappings(HierarchicalElementMapping parentMapping,
      Set<HierarchicalElementMapping> childMappings, Type type) {
    this.type = type;
    this.parentMapping = parentMapping;
    this.childMappings = Collections.unmodifiableSet(childMappings);
  }

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
    // always have a from attribute.
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

    // Go by all attribute mappings and figure out what to do with them. If an attribute mapping
    // does not contain a to attribute, assume it is the same as the from attribute.
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

    // Convert to hierarchical structure: find the parent mapping and filter unneeded mappings
    HierarchicalElementMapping[] parentMapping = new HierarchicalElementMapping[1];
    final Set<HierarchicalElementMapping> childMappings = new HashSet<>();
    addHierarchicalMappings(type, false, tagsWithoutContent, childMappings, parentMapping);
    addHierarchicalMappings(type, true, tagsWithContent, childMappings, parentMapping);
    if (parentMapping[0] == null) {
      throw new IllegalArgumentException(
          "There is no mapping to the parent tag: " + type.getMainTag().toString() + ".");
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

  public Type getType() {
    return type;
  }

  public HierarchicalElementMapping getParentMapping() {
    return parentMapping;
  }

  public Set<HierarchicalElementMapping> getChildMappings() {
    return childMappings;
  }

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
