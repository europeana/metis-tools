package eu.europeana.migration.metis.utils;

/**
 * This class provides some utils code in connection to testing for object equality and generating
 * hashcodes.
 * 
 * @author jochen
 *
 */
public final class ObjectIdentityUtils {

  private ObjectIdentityUtils() {}

  /**
   * Tests if two objects are equal using the objects' {@link Object#equals(Object)} method. This
   * method also tests for null values. Two null objects are considered equal.
   * 
   * @param object1 The first object.
   * @param object2 The second object.
   * @return Whether the two objects are equal.
   */
  public static <TYPE> boolean equalsIncludingNull(TYPE object1, TYPE object2) {
    return object1 == null ? object2 == null : object1.equals(object2);
  }

  private static int hashCodeIncludingNull(Object object) {
    return object == null ? 0 : object.hashCode();
  }

  /**
   * Generates a hashcode of a composite object (here represented by an array of objects).
   * 
   * @param objects The objects that should determine the hashcode.
   * @return The hashcode.
   */
  public static int hashCodeOfMultipleNullableObjects(Object... objects) {
    int hashCode = 0;
    for (Object object : objects) {
      hashCode = 31 * hashCode + hashCodeIncludingNull(object);
    }
    return hashCode;
  }
}
