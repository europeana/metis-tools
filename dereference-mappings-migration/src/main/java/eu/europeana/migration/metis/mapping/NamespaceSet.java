package eu.europeana.migration.metis.mapping;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import eu.europeana.migration.metis.utils.Namespace;

public enum NamespaceSet {

  GENERAL_OUTPUT(namespaceSet -> {
    addToSet(namespaceSet, Namespace.XML.getPrefix(), Namespace.XML.getUri());
    addToSet(namespaceSet, Namespace.RDF.getPrefix(), Namespace.RDF.getUri());
    addToSet(namespaceSet, Namespace.EDM.getPrefix(), Namespace.EDM.getUri());
    addToSet(namespaceSet, Namespace.OWL.getPrefix(), Namespace.OWL.getUri());
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, Namespace.WGS84_POS.getPrefix(), Namespace.WGS84_POS.getUri());
    addToSet(namespaceSet, Namespace.FOAF.getPrefix(), Namespace.FOAF.getUri());
    addToSet(namespaceSet, Namespace.RDAGR2.getPrefix(), Namespace.RDAGR2.getUri());
  }),

  GENERAL_INPUT(namespaceSet -> {
    addToSet(namespaceSet, Namespace.XML.getPrefix(), Namespace.XML.getUri());
    addToSet(namespaceSet, Namespace.RDF.getPrefix(), Namespace.RDF.getUri());
    addToSet(namespaceSet, "rdfs", "http://www.w3.org/2000/01/rdf-schema#");
  }),

  DNB(namespaceSet -> {
    addToSet(namespaceSet, "schema", "http://schema.org/");
    addToSet(namespaceSet, "gndo", "http://d-nb.info/standards/elementset/gnd#");
    addToSet(namespaceSet, "lib", "http://purl.org/library/");
    addToSet(namespaceSet, Namespace.OWL.getPrefix(), Namespace.OWL.getUri());
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, "geo", "http://www.opengis.net/ont/geosparql#");
    addToSet(namespaceSet, "umbel", "http://umbel.org/umbel#");
    addToSet(namespaceSet, "rdau", "http://rdaregistry.info/Elements/u/");
    addToSet(namespaceSet, "sf", "http://www.opengis.net/ont/sf#");
    addToSet(namespaceSet, "dcterms", "http://purl.org/dc/terms/");
    addToSet(namespaceSet, "vivo", "http://vivoweb.org/ontology/core#");
    addToSet(namespaceSet, "isbd", "http://iflastandards.info/ns/isbd/elements/");
    addToSet(namespaceSet, Namespace.FOAF.getPrefix(), Namespace.FOAF.getUri());
    addToSet(namespaceSet, "mo", "http://purl.org/ontology/mo/");
    addToSet(namespaceSet, "marcRole", "http://id.loc.gov/vocabulary/relators/");
    addToSet(namespaceSet, "dnba", "http://d-nb.info/standards/elementset/agrelon#");
    addToSet(namespaceSet, "dcmitype", "http://purl.org/dc/dcmitype/");
    addToSet(namespaceSet, "dbp", "http://dbpedia.org/property/");
    addToSet(namespaceSet, "dnbt", "http://d-nb.info/standards/elementset/dnb#");
    addToSet(namespaceSet, "dnb_intern", "http://dnb.de/");
    addToSet(namespaceSet, "v", "http://www.w3.org/2006/vcard/ns#");
    addToSet(namespaceSet, "ebu", "http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#");
    addToSet(namespaceSet, "bibo", "http://purl.org/ontology/bibo/");
    addToSet(namespaceSet, "gbv", "http://purl.org/ontology/gbv/");
    addToSet(namespaceSet, "dc", "http://purl.org/dc/elements/1.1/");
  }, "http://d-nb.info/gnd"),

  DISMARC(namespaceSet -> {
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, "dc", "http://purl.org/dc/elements/1.1/");
  }, "http://purl.org/dismarc/ns/"),

  EAGLE(namespaceSet -> {
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
  }, "http://www.eagle-network.eu/voc/material/skos/"),

  EUROPEANA(namespaceSet -> {
    addToSet(namespaceSet, "dc", "http://purl.org/dc/elements/1.1/");
    addToSet(namespaceSet, "dcterms", "http://purl.org/dc/terms/");
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
  }, "http://data.europeana.eu/concept/"),

  GEONAMES(namespaceSet -> {
    addToSet(namespaceSet, "cc", "http://creativecommons.org/ns#");
    addToSet(namespaceSet, "dcterms", "http://purl.org/dc/terms/");
    addToSet(namespaceSet, Namespace.FOAF.getPrefix(), Namespace.FOAF.getUri());
    addToSet(namespaceSet, "gn", "http://www.geonames.org/ontology#");
    addToSet(namespaceSet, Namespace.OWL.getPrefix(), Namespace.OWL.getUri());
    addToSet(namespaceSet, Namespace.WGS84_POS.getPrefix(), Namespace.WGS84_POS.getUri());
  }, "http://sws.geonames.org/"),

  GETTY(namespaceSet -> {
    addToSet(namespaceSet, "aat", "http://vocab.getty.edu/aat/");
    addToSet(namespaceSet, "aat_contrib", "http://vocab.getty.edu/aat/contrib/");
    addToSet(namespaceSet, "aat_rel", "http://vocab.getty.edu/aat/rel/");
    addToSet(namespaceSet, "aat_rev", "http://vocab.getty.edu/aat/rev/");
    addToSet(namespaceSet, "aat_scopeNote", "http://vocab.getty.edu/aat/scopeNote/");
    addToSet(namespaceSet, "aat_source", "http://vocab.getty.edu/aat/source/");
    addToSet(namespaceSet, "aat_source_rev", "http://vocab.getty.edu/aat/source/rev/");
    addToSet(namespaceSet, "aat_term", "http://vocab.getty.edu/aat/term/");
    addToSet(namespaceSet, "adms", "http://www.w3.org/ns/adms#");
    addToSet(namespaceSet, "bibo", "http://purl.org/ontology/bibo/");
    addToSet(namespaceSet, "bio", "http://purl.org/vocab/bio/0.1/");
    addToSet(namespaceSet, "cc", "http://creativecommons.org/ns#");
    addToSet(namespaceSet, "dc", "http://purl.org/dc/elements/1.1/");
    addToSet(namespaceSet, "dcat", "http://www.w3.org/ns/dcat#");
    addToSet(namespaceSet, "dct", "http://purl.org/dc/terms/");
    addToSet(namespaceSet, "dctype", "http://purl.org/dc/dcmitype/");
    addToSet(namespaceSet, "fmt", "http://www.w3.org/ns/formats/");
    addToSet(namespaceSet, Namespace.FOAF.getPrefix(), Namespace.FOAF.getUri());
    addToSet(namespaceSet, "freq", "http://purl.org/cld/freq/");
    addToSet(namespaceSet, "gvp", "http://vocab.getty.edu/ontology#");
    addToSet(namespaceSet, "gvp_lang", "http://vocab.getty.edu/language/");
    addToSet(namespaceSet, "iso", "http://purl.org/iso25964/skos-thes#");
    addToSet(namespaceSet, "luc", "http://www.ontotext.com/owlim/lucene#");
    addToSet(namespaceSet, "ontogeo", "http://www.ontotext.com/owlim/geo#");
    addToSet(namespaceSet, Namespace.OWL.getPrefix(), Namespace.OWL.getUri());
    addToSet(namespaceSet, "prov", "http://www.w3.org/ns/prov#");
    addToSet(namespaceSet, "ptop", "http://www.ontotext.com/proton/protontop#");
    addToSet(namespaceSet, "rr", "http://www.w3.org/ns/r2rml#");
    addToSet(namespaceSet, "rrx", "http://purl.org/r2rml-ext/");
    addToSet(namespaceSet, "schema", "http://schema.org/");
    addToSet(namespaceSet, "sd", "http://www.w3.org/ns/sparql-service-description#");
    addToSet(namespaceSet, "sesame", "http://www.openrdf.org/schema/sesame#");
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, "skosxl", "http://www.w3.org/2008/05/skos-xl#");
    addToSet(namespaceSet, "tgn", "http://vocab.getty.edu/tgn/");
    addToSet(namespaceSet, "tgn_contrib", "http://vocab.getty.edu/tgn/contrib/");
    addToSet(namespaceSet, "tgn_rel", "http://vocab.getty.edu/tgn/rel/");
    addToSet(namespaceSet, "tgn_rev", "http://vocab.getty.edu/tgn/rev/");
    addToSet(namespaceSet, "tgn_scopeNote", "http://vocab.getty.edu/tgn/scopeNote/");
    addToSet(namespaceSet, "tgn_source", "http://vocab.getty.edu/tgn/source/");
    addToSet(namespaceSet, "tgn_source_rev", "http://vocab.getty.edu/tgn/source/rev/");
    addToSet(namespaceSet, "tgn_term", "http://vocab.getty.edu/tgn/term/");
    addToSet(namespaceSet, "ulan", "http://vocab.getty.edu/ulan/");
    addToSet(namespaceSet, "ulan_bio", "http://vocab.getty.edu/ulan/bio/");
    addToSet(namespaceSet, "ulan_contrib", "http://vocab.getty.edu/ulan/contrib/");
    addToSet(namespaceSet, "ulan_event", "http://vocab.getty.edu/ulan/event/");
    addToSet(namespaceSet, "ulan_rel", "http://vocab.getty.edu/ulan/rel/");
    addToSet(namespaceSet, "ulan_rev", "http://vocab.getty.edu/ulan/rev/");
    addToSet(namespaceSet, "ulan_scopeNote", "http://vocab.getty.edu/ulan/scopeNote/");
    addToSet(namespaceSet, "ulan_source", "http://vocab.getty.edu/ulan/source/");
    addToSet(namespaceSet, "ulan_source_rev", "http://vocab.getty.edu/ulan/source/rev/");
    addToSet(namespaceSet, "ulan_term", "http://vocab.getty.edu/ulan/term/");
    addToSet(namespaceSet, "vaem", "http://www.linkedmodel.org/schema/vaem#");
    addToSet(namespaceSet, "vann", "http://purl.org/vocab/vann/");
    addToSet(namespaceSet, "vcard", "http://www.w3.org/2006/vcard/ns#");
    addToSet(namespaceSet, "vdpp", "http://data.lirmm.fr/ontologies/vdpp#");
    addToSet(namespaceSet, "voaf", "http://purl.org/vocommons/voaf#");
    addToSet(namespaceSet, "voag", "http://voag.linkedmodel.org/voag#");
    addToSet(namespaceSet, "void", "http://rdfs.org/ns/void#");
    addToSet(namespaceSet, "wdrs", "http://www.w3.org/2007/05/powder-s#");
    addToSet(namespaceSet, "wgs", Namespace.WGS84_POS.getUri());
    addToSet(namespaceSet, "wv", "http://vocab.org/waiver/terms/");
    addToSet(namespaceSet, "xsd", "http://www.w3.org/2001/XMLSchema#");
  }, "http://vocab.getty.edu/"),

  ICONCLASS(namespaceSet -> {
    addToSet(namespaceSet, "dc", "http://purl.org/dc/elements/1.1/");
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
  }, "http://www.imj.org.il/imagine/thesaurus/objects"),

  ISRAEL_MUSEUM(namespaceSet -> {
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, "dcterms", "http://purl.org/dc/terms/");
    addToSet(namespaceSet, Namespace.OWL.getPrefix(), Namespace.OWL.getUri());
    addToSet(namespaceSet, "shic", "http://light.demon.co.uk/shic.rdf#");
    addToSet(namespaceSet, "imj",
        "http://www.imj.org.il/imagine/thesaurus/objects/ObjectClassification.rdf#");
  }, "http://iconclass.org/"),

  MIMO(namespaceSet -> {
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, Namespace.FOAF.getPrefix(), Namespace.FOAF.getUri());
    addToSet(namespaceSet, Namespace.OWL.getPrefix(), Namespace.OWL.getUri());
    addToSet(namespaceSet, "rdaGr2", Namespace.RDAGR2.getUri());
    addToSet(namespaceSet, "rdaEnt", "http://RDVocab.info/uri/schema/FRBRentitiesRDA/");
    addToSet(namespaceSet, "dc", "http://purl.org/dc/elements/1.1/");
  }, "http://www.mimo-db.eu/"),

  PARTAGE(namespaceSet -> {
    addToSet(namespaceSet, "dcterms", "http://purl.org/dc/terms/");
    addToSet(namespaceSet, Namespace.FOAF.getPrefix(), Namespace.FOAF.getUri());
    addToSet(namespaceSet, "iso-thes", "http://purl.org/iso25964/skos-thes#");
    addToSet(namespaceSet, "vocnet", "http://schema.vocnet.org/");
    addToSet(namespaceSet, "xe", "http://xe.vocnet.org/");
    addToSet(namespaceSet, Namespace.OWL.getPrefix(), Namespace.OWL.getUri());
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, "xl", "http://www.w3.org/2008/05/skos-xl#");
    addToSet(namespaceSet, "crm", "http://www.cidoc-crm.org/cidoc-crm/");
    addToSet(namespaceSet, "isocat", "http://www.isocat.org/datcat/");
    addToSet(namespaceSet, "schema", "https://schema.org/");
    addToSet(namespaceSet, "lt", "http://terminology.lido-schema.org/");
  }, "http://partage.vocnet.org/"),

  PHOTO_CONSORTIUM(namespaceSet -> {
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, Namespace.OWL.getPrefix(), Namespace.OWL.getUri());
    addToSet(namespaceSet, "dct", "http://purl.org/dc/terms/");
    addToSet(namespaceSet, Namespace.FOAF.getPrefix(), "http://xmlns.com/foaf/spec/");
    addToSet(namespaceSet, "coll", "http://bib.arts.kuleuven.be/photoVocabulary/collections/");
    addToSet(namespaceSet, "schema", "http://bib.arts.kuleuven.be/photoVocabulary/schema#");
  }, "http://bib.arts.kuleuven.be/photoVocabulary/"),

  RDA(namespaceSet -> {
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, "dc", "http://purl.org/dc/elements/1.1/");
    addToSet(namespaceSet, "reg", "http://metadataregistry.org/uri/schema/registry/");
  }, "http://rdaregistry.info/termList/RDACarrierType"),

  UDC(namespaceSet -> {
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, Namespace.OWL.getPrefix(), Namespace.OWL.getUri());
    addToSet(namespaceSet, "dcterms", "http://purl.org/dc/terms/");
    addToSet(namespaceSet, "crm", "http://erlangen-crm.org/101001/");
    addToSet(namespaceSet, "udc", "http://udcdata.info/udc-schema#");
  }, "http://udcdata.info/rdf"),

  UNESCO(namespaceSet -> {
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, "isothes", "http://purl.org/iso25964/skos-thes#");
    addToSet(namespaceSet, "dc", "http://purl.org/dc/terms/");
  }, "http://vocabularies.unesco.org/thesaurus/"),

  VIAF(namespaceSet -> {
    addToSet(namespaceSet, "rdaGr2", Namespace.RDAGR2.getUri());
    addToSet(namespaceSet, "umbel", "http://umbel.org/umbel#");
    addToSet(namespaceSet, Namespace.SKOS.getPrefix(), Namespace.SKOS.getUri());
    addToSet(namespaceSet, Namespace.FOAF.getPrefix(), Namespace.FOAF.getUri());
    addToSet(namespaceSet, "genont", "http://www.w3.org/2006/gen/ont#");
    addToSet(namespaceSet, "pto", "http://www.productontology.org/id/");
    addToSet(namespaceSet, "dcterms", "http://purl.org/dc/terms/");
    addToSet(namespaceSet, "re", "http://oclcsrw.google.code/redirect");
    addToSet(namespaceSet, "void", "http://rdfs.org/ns/void#");
    addToSet(namespaceSet, "bgn", "http://bibliograph.net/");
    addToSet(namespaceSet, "schema", "http://schema.org/");
  }, "http://viaf.org/viaf/");

  public static final NamespaceCollection OUTPUT_COLLECTION =
      new NamespaceCollection(Collections.singleton(GENERAL_OUTPUT));

  private final Set<URI> urls;

  private final Set<Namespace> namespaces = new HashSet<>();

  private NamespaceSet(NamespacePopulator populator, String... urls) {
    this.urls = Arrays.stream(urls).map(NamespaceSet::convertToUri).collect(Collectors.toSet());
    populator.populateNamespaceSet(namespaces);
  }

  public Set<Namespace> getNamespaces() {
    return Collections.unmodifiableSet(namespaces);
  }

  private interface NamespacePopulator {
    public void populateNamespaceSet(Set<Namespace> namespaceSet);
  }

  private boolean appliesToUri(URI requestedUri) {
    return this.urls.stream().anyMatch(uri -> requestedUri.toString().startsWith(uri.toString()));
  }

  private static URI convertToUri(String uri) {
    try {
      return new URI(uri.trim()).normalize();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Could not parse uri: " + uri, e);
    }
  }

  public static NamespaceCollection getCollectionForInput(String uriString) {
    final URI uri = convertToUri(uriString);
    final Set<NamespaceSet> result = Arrays.stream(values())
        .filter(collection -> collection.appliesToUri(uri)).collect(Collectors.toSet());
    result.add(GENERAL_INPUT);
    return new NamespaceCollection(result);
  }

  private static void addToSet(Set<Namespace> namespaceSet, String prefix, String uri) {
    final boolean added = namespaceSet.add(new Namespace(prefix, uri));
    if (!added) {
      throw new IllegalArgumentException(
          "Could not add namespace ( " + prefix + " , " + uri + " ): already present in set.");
    }
  }
}
