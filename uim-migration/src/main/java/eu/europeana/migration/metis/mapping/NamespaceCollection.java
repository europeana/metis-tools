package eu.europeana.migration.metis.mapping;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import eu.europeana.migration.metis.utils.Namespace;

public class NamespaceCollection {

  private static final Comparator<Map.Entry<String, Namespace>> DECREASING_LENGTH_COMPARATOR =
      (entry1, entry2) -> entry2.getKey().length() - entry1.getKey().length();

  private final Map<String, Namespace> collection;

  public NamespaceCollection(Set<NamespaceSet> namespaceSets) {
    this.collection = namespaceSets.stream().map(NamespaceSet::getNamespaces).flatMap(Set::stream)
        .collect(Collectors.toConcurrentMap(Namespace::getPrefix, namespace -> namespace));
  }

  /**
   * Returns the longest prefix that matches the string (i.e. that the string begins with).
   * 
   * @param input The input string
   * @param separator The saparator that is required to be present after the prefix.
   * @return the prefix, or null if the string does not start with a known prefix.
   */
  public Namespace startsWithKnownPrefix(String input, String separator) {
    return collection.entrySet().stream()
        .filter(entry -> input.startsWith(entry.getKey() + separator))
        .sorted(DECREASING_LENGTH_COMPARATOR).map(Map.Entry::getValue).findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            "String does not start with a known prefix: " + input));
  }
}
