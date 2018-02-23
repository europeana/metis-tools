package eu.europeana.migration.metis;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.xml.stream.XMLStreamException;
import eu.europeana.corelib.dereference.impl.ControlledVocabularyImpl;
import eu.europeana.metis.dereference.Vocabulary;
import eu.europeana.migration.metis.mapping.ElementMappings;
import eu.europeana.migration.metis.utils.LogUtils;
import eu.europeana.migration.metis.xsl.XSLWriter;

/**
 * This class converts an old-style (UIM) vocabulary mapping to a new Metis one.
 * 
 * @author jochen
 *
 */
public class VocabularyConversion {

  private static final Charset XSL_ENCODING = StandardCharsets.UTF_8;

  private VocabularyConversion() {}

  /**
   * Convert a UIM vocabulary mapping to a Metis one.
   * 
   * @param uimVocabulary The UIM vocabulary mapping to convert.
   * @return The converted (Metis) vocabulary. Is not null.
   * @throws XMLStreamException
   * @throws IOException
   */
  public static Vocabulary convert(ControlledVocabularyImpl uimVocabulary)
      throws XMLStreamException, IOException {

    // Create object and set simple properties
    final Vocabulary vocabulary = new Vocabulary();
    vocabulary.setId(uimVocabulary.getId().toString());
    vocabulary.setUri(uimVocabulary.getURI());
    vocabulary.setIterations(uimVocabulary.getIterations());
    vocabulary.setName(uimVocabulary.getName());
    vocabulary.setSuffix(uimVocabulary.getSuffix());

    // Split the rules into sets of url rules and type rules and save them.
    final Set<String> typeRules = uimVocabulary.getRules() == null ? new HashSet<>()
        : Arrays.stream(uimVocabulary.getRules()).map(String::trim).collect(Collectors.toSet());
    final Set<String> urlRules =
        typeRules.stream().filter(rule -> !rule.matches("^<#.*>$")).collect(Collectors.toSet());
    typeRules.removeAll(urlRules);
    vocabulary.setRules(urlRules);
    vocabulary.setTypeRules(typeRules);

    // Reading and parsing element mappings and save them as XSL
    final ElementMappings elementMappings = ElementMappings.create(uimVocabulary);
    vocabulary.setXslt(new String(XSLWriter.writeToXSL(elementMappings), XSL_ENCODING));
    vocabulary.setType(elementMappings.getType().getMetisType());

    // Done
    LogUtils.logInfoMessage("Type rules: " + vocabulary.getTypeRules());
    LogUtils.logInfoMessage("URL rules : " + vocabulary.getRules());
    LogUtils.logInfoMessage("Mappings:");
    LogUtils.logInfoMessage(elementMappings.toString());
    return vocabulary;
  }
}
