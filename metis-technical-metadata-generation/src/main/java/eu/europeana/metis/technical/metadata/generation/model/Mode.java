package eu.europeana.metis.technical.metadata.generation.model;

/**
 * Mode options.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-19
 */
public enum Mode {
  DEFAULT,
  RETRY_FAILED,
  UPLOAD_THUMBNAILS;

  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    throw new IllegalArgumentException();
  }

}
