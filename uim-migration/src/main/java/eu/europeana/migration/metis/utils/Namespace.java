package eu.europeana.migration.metis.utils;

public class Namespace {

  public static final Namespace XML = new Namespace("xml", "http://www.w3.org/XML/1998/namespace");
  public static final Namespace XSL = new Namespace("xsl", "http://www.w3.org/1999/XSL/Transform");
  public static final Namespace RDF =
      new Namespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
  public static final Namespace EDM = new Namespace("edm", "http://www.europeana.eu/schemas/edm");
  public static final Namespace OWL = new Namespace("owl", "http://www.w3.org/2002/07/owl#");
  public static final Namespace SKOS =
      new Namespace("skos", "http://www.w3.org/2004/02/skos/core#");
  public static final Namespace WGS84_POS =
      new Namespace("wgs84_pos", "http://www.w3.org/2003/01/geo/wgs84_pos#");
  public static final Namespace FOAF = new Namespace("foaf", "http://xmlns.com/foaf/0.1/");
  public static final Namespace RDAGR2 =
      new Namespace("rdagr2", "http://RDVocab.info/ElementsGr2/");

  private final String prefix;
  private final String uri;

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

  public String getPrefix() {
    return prefix;
  }

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
}
