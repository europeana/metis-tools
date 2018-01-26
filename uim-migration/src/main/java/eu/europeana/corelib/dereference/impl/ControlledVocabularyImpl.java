package eu.europeana.corelib.dereference.impl;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

@Entity("ControlledVocabulary")
public class ControlledVocabularyImpl implements Serializable {

  private static final long serialVersionUID = 1L;

  @Id
  private ObjectId id;

  private String URI;
  private String name;
  private String location;
  private String[] rules;
  private int iterations;

  // Used to denote special characteristics of the Resource. For example the
  // Geonames always point to URI/ResourceCode/about.rdf
  // rather than URI/ResourceCode which redirects to the Geonames website.
  private String suffix;
  @Embedded
  private Map<String, List<EdmMappedField>> elements;
  private String replaceUrl;

  public ControlledVocabularyImpl() {

  }

  public ControlledVocabularyImpl(String name) {
    this.name = name;
  }

  public ObjectId getId() {
    return id;
  }

  public void setId(ObjectId id) {
    this.id = id;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public void setSuffix(String suffix) {
    this.suffix = suffix;
  }

  public String getSuffix() {
    return this.suffix;
  }

  public void setURI(String uri) {
    this.URI = uri;
  }

  public String getURI() {
    return this.URI;
  }

  public Map<String, List<EdmMappedField>> getElements() {
    return this.elements;
  }

  public void setElements(Map<String, List<EdmMappedField>> elements) {
    this.elements = elements;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String[] getRules() {
    return rules;
  }

  public void setRules(String[] rules) {
    this.rules = rules != null ? rules.clone() : null;
  }

  public void setIterations(int iterations) {
    this.iterations = iterations;
  }

  public int getIterations() {
    return this.iterations;
  }

  public void setReplaceUrl(String replaceUrl) {
    this.replaceUrl = replaceUrl;
  }

  public String getReplaceUrl() {
    return this.replaceUrl;
  }

}
