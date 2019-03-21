package eu.europeana.migration.metis.utils;

/**
 * <p>
 * This class represents an XML namespace, consisting of a prefix and a namespace uri.
 * </p>
 * <p>
 * Some popular namespaces are predefined as constants in this class, but this list is by no means
 * exclusive.
 * </p>
 * <p>
 * Equality is defined as having the same prefix. Instances of this class are meant to be used in
 * the context of XML files, where a namespace uri may occur multiple times with different prefixes.
 * </p>
 * 
 * @author jochen
 *
 */
public class Namespace {

  /** The XML namespace. **/
  public static final Namespace XML = new Namespace("xml", "http://www.w3.org/XML/1998/namespace");

  /** The XSL namespace. **/
  public static final Namespace XSL = new Namespace("xsl", "http://www.w3.org/1999/XSL/Transform");

  /** The RDF namespace. **/
  public static final Namespace RDF =
      new Namespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");

  /** The EDM namespace. **/
  public static final Namespace EDM = new Namespace("edm", "http://www.europeana.eu/schemas/edm/") {
    @Override
    public String capitalizeTagName(String tagName) {
      switch (tagName) {
        case "agent":
          return "Agent";
        case "place":
          return "Place";
        case "timeSpan":
        case "timespan":
          return "TimeSpan";
        default:
          return super.capitalizeTagName(tagName);
      }
    }
  };

  /** The OWL namespace. **/
  public static final Namespace OWL = new Namespace("owl", "http://www.w3.org/2002/07/owl#");

  /** The SKOS namespace. **/
  public static final Namespace SKOS =
      new Namespace("skos", "http://www.w3.org/2004/02/skos/core#") {
        @Override
        public String capitalizeTagName(String tagName) {
          switch (tagName) {
            case "concept":
              return "Concept";
            default:
              return super.capitalizeTagName(tagName);
          }
        }
      };

  /** The WGS84_POS namespace. **/
  public static final Namespace WGS84_POS =
      new Namespace("wgs84_pos", "http://www.w3.org/2003/01/geo/wgs84_pos#");

  /** The FOAF namespace. **/
  public static final Namespace FOAF = new Namespace("foaf", "http://xmlns.com/foaf/0.1/");

  /** The RDAGR2 namespace. **/
  public static final Namespace RDAGR2 =
      new Namespace("rdagr2", "http://RDVocab.info/ElementsGr2/");

  private final String prefix;
  private final String uri;

  /**
   * Constructor
   * 
   * @param prefix The namespace prefix.
   * @param uri The namespace uri.
   */
  public Namespace(String prefix, String uri) {
    if (prefix == null || prefix.trim().isEmpty()) {
      throw new IllegalArgumentException("Prefix cannot be null or empty: " + prefix);
    }
    if (uri == null || uri.trim().isEmpty()) {
      throw new IllegalArgumentException("Tag name cannot be null or empty:" + uri);
    }
    this.prefix = prefix.trim();
    this.uri = uri.trim();
  }

  /**
   * 
   * @return the namespace prefix.
   */
  public String getPrefix() {
    return prefix;
  }

  /**
   * 
   * @return the namespace uri.
   */
  public String getUri() {
    return uri;
  }

  @Override
  public boolean equals(Object otherObject) {
    if (!(otherObject instanceof Namespace)) {
      return false;
    }
    return getPrefix().equals(((Namespace) otherObject).getPrefix());
  }

  @Override
  public int hashCode() {
    return prefix.hashCode();
  }

  /**
   * Allows for capitalizing tag names that need to be capitalized. Default behavior: return the tag
   * name unchanged. Subclasses/instances can override.
   * 
   * @param tagName The tag name.
   * @return The capitalized tag name.
   */
  public String capitalizeTagName(String tagName) {
    return tagName;
  }
}
