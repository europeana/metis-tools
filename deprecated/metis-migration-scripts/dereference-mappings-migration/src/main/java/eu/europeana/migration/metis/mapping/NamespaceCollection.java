package eu.europeana.migration.metis.mapping;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import eu.europeana.migration.metis.utils.Namespace;

/**
 * This class represents a collection of namespaces that are allowed for tags to have in a given
 * context.
 * 
 * @author jochen
 *
 */
public class NamespaceCollection {

  private static final Comparator<Map.Entry<String, Namespace>> DECREASING_LENGTH_COMPARATOR =
      (entry1, entry2) -> entry2.getKey().length() - entry1.getKey().length();

  private final Map<String, Namespace> collection;

  /**
   * Constructor.
   *
   * @param namespaceSets The set of namespaces.
   */
  public NamespaceCollection(Set<NamespaceSet> namespaceSets) {
    this.collection = namespaceSets.stream().map(NamespaceSet::getNamespaces).flatMap(Set::stream)
        .collect(Collectors.toConcurrentMap(Namespace::getPrefix, namespace -> namespace));
  }

  /**
   * <p>
   * Returns the namespace to which this element (given in string form) belongs. This method throws
   * an exception if the element does not belong to any of the permissible namespaces.
   * </p>
   * <p>
   * The namespace is determined by finding the longest known prefix that matches the string (i.e.
   * that the string begins with), requiring that the prefix is followed immediately by the
   * separator.
   * </p>
   * 
   * @param elementString The element as a string
   * @param separator The separator that is required to be present after the prefix.
   * @return the namespace.
   */
  public Namespace getNamespaceForTag(String elementString, String separator) {
    return collection.entrySet().stream()
        .filter(entry -> elementString.startsWith(entry.getKey() + separator))
        .sorted(DECREASING_LENGTH_COMPARATOR).map(Map.Entry::getValue).findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            "String does not start with a known prefix: " + elementString));
  }
}
