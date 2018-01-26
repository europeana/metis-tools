package eu.europeana.migration.metis.utils;

public final class MigrationUtils {
  private MigrationUtils() {}

  public static <TYPE> boolean equalsIncludingNull(TYPE object1, TYPE object2) {
    return object1 == null ? object2 == null : object1.equals(object2);
  }

  public static int hashCodeIncludingNull(Object object) {
    return object == null ? 0 : object.hashCode();
  }

  public static int hashCodeOfMultipleNullableObjects(Object... objects) {
    int hashCode = 0;
    for (Object object : objects) {
      hashCode = 31 * hashCode + hashCodeIncludingNull(object);
    }
    return hashCode;
  }
}
