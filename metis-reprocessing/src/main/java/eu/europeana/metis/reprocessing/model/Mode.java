package eu.europeana.metis.reprocessing.model;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-19
 */
public enum Mode {
  DEFAULT,
  REPROCESS_ALL_FAILED;

  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    return DEFAULT;
  }

}
