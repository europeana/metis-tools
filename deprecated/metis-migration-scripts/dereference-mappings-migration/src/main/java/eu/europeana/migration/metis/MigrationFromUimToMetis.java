package eu.europeana.migration.metis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerFactoryConfigurationError;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import eu.europeana.corelib.dereference.impl.ControlledVocabularyImpl;
import eu.europeana.corelib.dereference.impl.EdmMappedField;
import eu.europeana.metis.dereference.Vocabulary;
import eu.europeana.migration.metis.utils.LogUtils;
import eu.europeana.migration.metis.utils.MigrationProperties;

/**
 * This class performs the migration of vocabulary mappings from UIM to Metis.
 * 
 * @author jochen
 *
 */
public class MigrationFromUimToMetis {

  private static final Set<String> SKIP_VOCABULARY_IDS =
      Arrays.stream(new String[] {"5a378f52e4b00c2521a530c0"}).collect(Collectors.toSet());

  private static final Map<String, Consumer<ControlledVocabularyImpl>> VOCABULARY_CORRECTIONS =
      new HashMap<>();
  static {

    // UDC: rewire skos:prefLabel to map to itself instead of skos:Concept and vv.
    VOCABULARY_CORRECTIONS.put("599a907be4b0acaf76fc33d0", vocabulary -> {
      vocabulary.getElements().remove("skos:prefLabel");
      vocabulary.getElements().remove("skos:Concept");
      vocabulary.getElements().put("skos:prefLabel",
          Arrays.asList(new EdmMappedField("cc_skos_prefLabel", null)));
    });

    // ULAN: add parent tag: gvp:Subject
    VOCABULARY_CORRECTIONS.put("5a09acd4e4b0b4ef773ca22e", vocabulary -> {
      vocabulary.getElements().put("gvp:Subject",
          Arrays.asList(new EdmMappedField("edm_agent", null)));
      vocabulary.getElements().put("gvp:Subject_rdf:about",
          Arrays.asList(new EdmMappedField("edm_agent", null)));
    });

    // AAT: add about attribute for parent tag
    VOCABULARY_CORRECTIONS.put("5a0c0355e4b0b4ef773ca38c", vocabulary -> vocabulary.getElements()
        .put("gvp:Subject_rdf:about", Arrays.asList(new EdmMappedField("skos_concept", null))));

    // TGN places
    VOCABULARY_CORRECTIONS.put("5a28148be4b00c2521a52eea", vocabulary -> vocabulary.getElements()
        .put("gvp:Subject_rdf:about", Arrays.asList(new EdmMappedField("edm_place", null))));
  }

  /**
   * The main method performing the vocabulary mappings.
   * 
   * @param args the arguments. If the first argument exists it is assumed that it is an alternative
   *        location for the configuration file (see {@link MigrationProperties}).
   */
  public static void main(String[] args) {
    try {
      mainInternal(args);
    } catch (Exception exception) {
      LogUtils.logException(exception);
    }
  }

  private static void mainInternal(String[] args)
      throws IOException, XMLStreamException, TransformerFactoryConfigurationError {

    // Load properties file
    final String propertiesFile = args.length > 0 ? args[0] : null;
    final MigrationProperties properties = MigrationProperties.readFromFile(propertiesFile);

    // Load all the vocabularies from the source database
    final List<ControlledVocabularyImpl> uimVocabularies = getVocabulariesFromSource(properties);

    // Process the vocabularies
    final List<Vocabulary> metisVocabularies = new ArrayList<>();
    for (ControlledVocabularyImpl uimVocabulary : uimVocabularies) {

      // Begin process. Skip this vocabulary if needed.
      LogUtils.logInfoMessage("==============================");
      if (SKIP_VOCABULARY_IDS.contains(uimVocabulary.getId().toString())) {
        LogUtils.logInfoMessage("Skipping vocabulary " + uimVocabulary.getName() + " (id: "
            + uimVocabulary.getId().toString() + ") - this vocabulary seems to be broken.");
        continue;
      }
      LogUtils.logInfoMessage("Processing vocabulary " + uimVocabulary.getName() + " (id: "
          + uimVocabulary.getId().toString() + ").");

      // Applying vocabulary corrections
      final Consumer<ControlledVocabularyImpl> correction =
          VOCABULARY_CORRECTIONS.get(uimVocabulary.getId().toString());
      if (correction != null) {
        correction.accept(uimVocabulary);
      }

      // Converting to Metis vocabulary
      final Vocabulary vocabulary = VocabularyConversion.convert(uimVocabulary);
      metisVocabularies.add(vocabulary);
    }
    LogUtils.logInfoMessage("==============================");

    // save resulting vocabulary
    LogUtils.logInfoMessage("Saving resulting vocabularies.");
    saveVocabulariesToDestination(metisVocabularies, properties);

  }

  private static List<ControlledVocabularyImpl> getVocabulariesFromSource(
      MigrationProperties properties) {
    final Morphia morphia = new Morphia().map(ControlledVocabularyImpl.class);
    try (MongoClient client = new MongoClient(new MongoClientURI(properties.getSourceMongoUrl()))) {
      final Datastore datastore =
          morphia.createDatastore(client, properties.getSourceMongoDbName());
      datastore.ensureIndexes();
      return datastore.createQuery(ControlledVocabularyImpl.class).asList();
    }
  }

  private static void saveVocabulariesToDestination(List<Vocabulary> vocabularies,
      MigrationProperties properties) {
    final Morphia morphia = new Morphia().map(Vocabulary.class);
    try (MongoClient client = new MongoClient(new MongoClientURI(properties.getTargetMongoUrl()))) {

      // Set up datastore
      final Datastore datastore =
          morphia.createDatastore(client, properties.getTargetMongoDbName());
      datastore.ensureIndexes();

      // Remove all existing data
      final List<Vocabulary> oldVocabularies = datastore.find(Vocabulary.class).asList();
      for (Vocabulary oldVocabulary : oldVocabularies) {
        datastore.delete(oldVocabulary);
      }

      // save the vocabularies.
      datastore.save(vocabularies);
    }
  }
}
