package eu.europeana.metis.removal.config;

/**
 * The enum Mode.
 */
public enum Mode {
  /**
   * Dry run mode.
   */
  DRY_RUN,
  /**
   * Default mode.
   */
  DEFAULT,
  /**
   * Reprocess all failed mode.
   */
  REPROCESS_ALL_FAILED,
  /**
   * Post clean mode.
   */
  POST_CLEAN,
  /**
   * Clean mode.
   */
  CLEAN;

  /**
   * Gets mode from enum name.
   *
   * @param enumName the enum name
   * @return the mode from enum name
   */
  public static Mode getModeFromEnumName(String enumName) {
    for (Mode mode : Mode.values()) {
      if (mode.name().equalsIgnoreCase(enumName)) {
        return mode;
      }
    }
    throw new IllegalArgumentException("Wrong Mode supplied");
  }
}
