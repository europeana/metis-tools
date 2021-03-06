package eu.europeana.metis.creator;

import java.util.List;

/**
 * @author Simon Tzanakis
 * @since 2020-08-04
 */
public class LabelInfo {

  private String lang;
  private List<String> originalLabel;
  private List<String> lowerCaseLabel;

  public LabelInfo() {
  }

  public LabelInfo(List<String> originalLabel, List<String> lowerCaseLabel, String lang) {
    this.originalLabel = originalLabel;
    this.lowerCaseLabel = lowerCaseLabel;
    this.lang = lang;
  }

  public String getLang() {
    return lang;
  }

  public void setLang(String lang) {
    this.lang = lang;
  }

  public List<String> getOriginalLabel() {
    return originalLabel;
  }

  public void setOriginalLabel(List<String> originalLabel) {
    this.originalLabel = originalLabel;
  }

  public List<String> getLowerCaseLabel() {
    return lowerCaseLabel;
  }

  public void setLowerCaseLabel(List<String> lowerCaseLabel) {
    this.lowerCaseLabel = lowerCaseLabel;
  }
}
