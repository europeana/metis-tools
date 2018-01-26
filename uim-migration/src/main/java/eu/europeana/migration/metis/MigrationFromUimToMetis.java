package eu.europeana.migration.metis;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.apache.commons.io.FileUtils;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import eu.europeana.corelib.dereference.impl.ControlledVocabularyImpl;
import eu.europeana.metis.dereference.Vocabulary;
import eu.europeana.migration.metis.mapping.ElementMappings;
import eu.europeana.migration.metis.mapping.Type;
import eu.europeana.migration.metis.utils.LogUtils;
import eu.europeana.migration.metis.utils.MigrationProperties;
import eu.europeana.migration.metis.xsl.XSLWriter;

public class MigrationFromUimToMetis {

  private static final Charset XSL_ENCODING = StandardCharsets.UTF_8;
  private static final String SAMPLE_DATA_OUTPUT_DIRECTORY = null;
  // private static final String SAMPLE_DATA_OUTPUT_DIRECTORY = "/home/jochen/Desktop/output/";

  // Only needed for test database
  private static final Set<String> SKIP_VOCABULARY_IDS =
      Arrays.stream(new String[] {}).collect(Collectors.toSet());

  public static void main(String[] args) {
    try {
      mainInternal(args);
    } catch (Exception exception) {
      LogUtils.logException(exception);
    }
  }

  private static void mainInternal(String[] args) throws IOException, XMLStreamException,
      TransformerException, TransformerFactoryConfigurationError {

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

      // Converting to Metis vocabulary
      final Vocabulary vocabulary = convert(uimVocabulary);
      metisVocabularies.add(vocabulary);

      // Apply transformation to sample data
      if (SAMPLE_DATA_OUTPUT_DIRECTORY == null) {
        applyXslToSampleData(vocabulary);
      }
    }
    LogUtils.logInfoMessage("==============================");

    // save resulting vocabulary
    LogUtils.logInfoMessage("Saving resulting vocabularies.");
    saveVocabulariesToDestination(metisVocabularies, properties);

  }

  private static Vocabulary convert(ControlledVocabularyImpl uimVocabulary)
      throws XMLStreamException, IOException {

    // Create object and set simple properties
    final Vocabulary vocabulary = new Vocabulary();
    vocabulary.setId(uimVocabulary.getId().toString());
    vocabulary.setURI(uimVocabulary.getURI());
    vocabulary.setIterations(uimVocabulary.getIterations());
    vocabulary.setName(uimVocabulary.getName());
    // TODO set suffix!

    // Split the rules into sets of url rules and type rules and save them.
    final Set<String> typeRules =
        Arrays.stream(uimVocabulary.getRules()).map(String::trim).collect(Collectors.toSet());
    final Set<String> urlRules =
        typeRules.stream().filter(rule -> !rule.matches("^<#.*>$")).collect(Collectors.toSet());
    typeRules.removeAll(urlRules);
    if (urlRules.isEmpty()) {
      urlRules.add("*");
    }
    // TODO why not make array of these two variables?
    vocabulary.setRules(urlRules.stream().collect(Collectors.joining(" ")));
    vocabulary.setTypeRules(typeRules.stream().collect(Collectors.joining(" ")));

    // Reading and parsing element mappings and save them as XSL
    final ElementMappings elementMappings = ElementMappings.create(uimVocabulary);
    vocabulary.setXslt(new String(XSLWriter.writeToXSL(elementMappings), XSL_ENCODING));
    vocabulary.setType(Type.convertToMetisType(elementMappings.getType()));

    // Done
    LogUtils.logInfoMessage("Type rules: " + vocabulary.getTypeRules());
    LogUtils.logInfoMessage("URL rules : " + vocabulary.getRules());
    LogUtils.logInfoMessage("Mappings:");
    LogUtils.logInfoMessage(elementMappings.toString());
    return vocabulary;
  }

  private static void applyXslToSampleData(Vocabulary vocabulary)
      throws TransformerException, IOException {

    // Save xsl to file.
    final File xslFile = new File(SAMPLE_DATA_OUTPUT_DIRECTORY + vocabulary.getId() + ".xsl");
    FileUtils.writeStringToFile(xslFile, vocabulary.getXslt(), XSL_ENCODING);

    // Transforming sample input
    final InputStream sample = MigrationFromUimToMetis.class.getClassLoader()
        .getResourceAsStream("samples/" + vocabulary.getId() + ".xml");
    if (sample != null) {
      LogUtils.logInfoMessage("Applying transformation.");
      final File outputFile = new File(SAMPLE_DATA_OUTPUT_DIRECTORY + vocabulary.getId() + ".xml");
      final Transformer transformer = TransformerFactory.newInstance().newTransformer(
          new StreamSource(new ByteArrayInputStream(vocabulary.getXslt().getBytes(XSL_ENCODING))));
      transformer.transform(new StreamSource(sample), new StreamResult(outputFile));
    } else {
      LogUtils.logInfoMessage("Could not apply transformation: sample file  not found.");
    }
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

      // save the vocabularies (overwriting previous data with the same ID).
      datastore.save(vocabularies);
    }
  }
}
