package eu.europeana.metis.tools.dataset.migration;

/**
 * The columns from the csv that are to be used.
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-03-13
 */
public enum Columns {
  NAME(0), ORNANIZATION_NAME(9), DESCRIPTION(10), DATE_CREATED(19), DATE_MODIFIED(20), METADATA_FORMAT(26), HARVEST_URL(43),
  COUNTRY(44), HTTP_URL(49), SETSPEC(58), NOTES(64), COUNTRY_CODE(66), DATASET_COUNTRY_CODE(68), HARVEST_TYPE(75);

  private int index;

  Columns(int index) {
    this.index = index;
  }

  public int getIndex() {
    return index;
  }
}
