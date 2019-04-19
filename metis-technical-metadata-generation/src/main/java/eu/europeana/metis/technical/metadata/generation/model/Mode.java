package eu.europeana.metis.technical.metadata.generation.model;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-19
 */
public enum Mode {
  DEFAULT,
  START_FROM_BEGINNING_IGNORE_PROCESSED,
  START_FROM_BEGINNING_RETRY_FAILED;

  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    return DEFAULT;
  }

}
