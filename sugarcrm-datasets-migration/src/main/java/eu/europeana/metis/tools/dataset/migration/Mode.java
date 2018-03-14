package eu.europeana.metis.tools.dataset.migration;

/**
 * The Mode that is used to run the script
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2018-03-14
 */
public enum Mode {
  CREATE, DELETE, NOT_VALID_MODE;

  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    return NOT_VALID_MODE;
  }
}
