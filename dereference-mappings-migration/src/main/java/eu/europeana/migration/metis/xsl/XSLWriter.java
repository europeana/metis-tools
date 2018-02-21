package eu.europeana.migration.metis.xsl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import eu.europeana.migration.metis.mapping.ElementMapping;
import eu.europeana.migration.metis.mapping.ElementMappings;
import eu.europeana.migration.metis.mapping.HierarchicalElementMapping;
import eu.europeana.migration.metis.utils.Namespace;

public class XSLWriter {

  private static final String TAG_SEPARATOR = ":";

  private static final String XSL_TAG_STYLESHEET = "stylesheet";
  private static final String XSL_TAG_PARAMETER = "param";
  private static final String XSL_TAG_OUTPUT = "output";
  private static final String XSL_TAG_TEMPLATE = "template";
  private static final String XSL_TAG_FOR_EACH = "for-each";
  private static final String XSL_TAG_IF = "if";
  private static final String XSL_TAG_ATTRIBUTE = "attribute";
  private static final String XSL_TAG_VALUE_OF = "value-of";
  private static final String XSL_TAG_TEXT = "text";

  private static final String XSL_ATTR_VERSION = "version";
  private static final String XSL_ATTR_NAME = "name";
  private static final String XSL_ATTR_SELECT = "select";
  private static final String XSL_ATTR_INDENT = "indent";
  private static final String XSL_ATTR_ENCODING = "encoding";
  private static final String XSL_ATTR_MATCH = "match";
  private static final String XSL_ATTR_TEST = "test";

  private static final String VALUE_XML_VERSION = "1.0";
  private static final String VALUE_XSL_VERSION = "1.0";
  private static final String VALUE_XSL_ATTR_PREFIX = "@";
  private static final String VALUE_XSL_DIRECT_CHILD_PREFIX = "./";
  private static final String VALUE_INDENT = "yes";
  private static final String VALUE_ENCODING = StandardCharsets.UTF_8.name();

  public static final String TARGET_ID_PARAMETER_NAME = "targetId";
  private static final String TARGET_ID_DEFAULT_VALUE = "";

  private static final String INCOMING_BASE_TAG = Namespace.RDF.getPrefix() + TAG_SEPARATOR + "RDF";

  private XSLWriter() {}

  public static byte[] writeToXSL(ElementMappings mappings) throws XMLStreamException, IOException {
    try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      final XMLOutputFactory outputFactory = XMLOutputFactory.newInstance();
      final XMLStreamWriter writer = outputFactory.createXMLStreamWriter(outputStream);
      writeToXSL(writer, mappings);
      writer.flush();
      writer.close();
      return outputStream.toByteArray();
    }
  }

  private static Set<Namespace> getNamespaceDeclarations(ElementMappings mappings) {

    // Collect all result from mappings.
    final Set<Namespace> result = new HashSet<>();
    addNamespaces(mappings.getParentMapping(), result);
    for (HierarchicalElementMapping childMapping : mappings.getChildMappings()) {
      addNamespaces(childMapping, result);
    }

    // Remove XML namespace and add XSL namespace
    result.add(Namespace.XSL);
    result.remove(Namespace.XML);

    // Done
    return result;
  }

  private static void addNamespaces(HierarchicalElementMapping mapping, Set<Namespace> result) {
    result.add(mapping.getTagMapping().getFrom().getNamespace());
    result.add(mapping.getTagMapping().getTo().getNamespace());
    mapping.getAttributeMappings().stream().forEach(attribute -> {
      result.add(attribute.getFrom().getNamespace());
      result.add(attribute.getTo().getNamespace());
    });
  }

  private static void writeToXSL(XMLStreamWriter writer, ElementMappings mappings)
      throws XMLStreamException {

    // Collect all required namespace declarations.
    final Set<Namespace> namespaceDeclarations = getNamespaceDeclarations(mappings);

    // Write the start of the document (stylesheet tag) including all namespaces.
    writer.writeStartDocument(StandardCharsets.UTF_8.name(), VALUE_XML_VERSION);
    writer.writeStartElement(Namespace.XSL.getPrefix(), XSL_TAG_STYLESHEET, Namespace.XSL.getUri());
    writer.writeAttribute(XSL_ATTR_VERSION, VALUE_XSL_VERSION);
    for (Namespace namespaceDeclaration : namespaceDeclarations) {
      writer.writeNamespace(namespaceDeclaration.getPrefix(), namespaceDeclaration.getUri());
    }

    // Write input parameter for rdf:about ID.
    writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_PARAMETER);
    writer.writeAttribute(XSL_ATTR_NAME, TARGET_ID_PARAMETER_NAME);
    writer.writeAttribute(XSL_ATTR_SELECT, TARGET_ID_DEFAULT_VALUE);
    writer.writeEndElement();

    // Write output directive
    writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_OUTPUT);
    writer.writeAttribute(XSL_ATTR_INDENT, VALUE_INDENT);
    writer.writeAttribute(XSL_ATTR_ENCODING, VALUE_ENCODING);
    writer.writeEndElement();

    // Write template (delegate contents).
    writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_TEMPLATE);
    writer.writeAttribute(XSL_ATTR_MATCH, "/" + INCOMING_BASE_TAG);
    writeTemplateContents(writer, mappings);
    writer.writeEndElement();

    // Write the end of the document (stylesheet tag).
    writer.writeEndElement();
    writer.writeEndDocument();

  }

  private static void writeTemplateContents(XMLStreamWriter writer, ElementMappings mappings)
      throws XMLStreamException {

    // Write comment
    final ElementMapping parentTagMapping = mappings.getParentMapping().getTagMapping();
    writer.writeComment(" Parent mapping: " + parentTagMapping.toString() + " ");
    
    // Start parent tag (including for-each).
    writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_FOR_EACH);
    writer.writeAttribute(XSL_ATTR_SELECT,
        VALUE_XSL_DIRECT_CHILD_PREFIX + parentTagMapping.getFrom().toString(TAG_SEPARATOR));
    writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_IF);
    writer.writeAttribute(XSL_ATTR_TEST, VALUE_XSL_ATTR_PREFIX
        + mappings.getDocumentIdMapping().toString() + "=$" + TARGET_ID_PARAMETER_NAME);
    writer.writeStartElement(parentTagMapping.getTo().getNamespace().getUri(),
        parentTagMapping.getTo().getTagName());

    // Write attributes of parent mapping
    writeAttributes(writer, mappings.getParentMapping().getAttributeMappings());

    // Write child mappings
    for (HierarchicalElementMapping childMapping : mappings.getChildMappings()) {
      writeChildMapping(writer, childMapping);
    }

    // End parent tag (including for-each).
    writer.writeEndElement();
    writer.writeEndElement();
    writer.writeEndElement();
  }

  private static void writeChildMapping(XMLStreamWriter writer,
      HierarchicalElementMapping childMapping) throws XMLStreamException {

    // Write comment
    writer.writeComment(" Tag mapping: " + childMapping.getTagMapping().toString() + " ");

    // Start child tag (including for-each).
    writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_FOR_EACH);
    writer.writeAttribute(XSL_ATTR_SELECT, VALUE_XSL_DIRECT_CHILD_PREFIX
        + childMapping.getTagMapping().getFrom().toString(TAG_SEPARATOR));
    writer.writeStartElement(childMapping.getTagMapping().getTo().getNamespace().getUri(),
        childMapping.getTagMapping().getTo().getTagName());

    // Write attributes of child tag
    writeAttributes(writer, childMapping.getAttributeMappings());

    // Write tag value if needed
    if (childMapping.isIncludeValueOfTag()) {
      writeTagValue(writer);
    }

    // End child tag (including for-each).
    writer.writeEndElement();
    writer.writeEndElement();
  }

  private static void writeTagValue(XMLStreamWriter writer) throws XMLStreamException {

    // Write comment
    writer.writeComment(" Text content mapping (only for non-space-only content) ");

    // Start for-each on all child text nodes and normalize.
    writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_FOR_EACH);
    writer.writeAttribute(XSL_ATTR_SELECT, "text()[normalize-space()]");

    // Add space between different text nodes.
    writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_IF);
    writer.writeAttribute(XSL_ATTR_TEST, "position() > 1");
    writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_TEXT);
    writer.writeCharacters(" ");
    writer.writeEndElement();
    writer.writeEndElement();

    // Write the text node.
    writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_VALUE_OF);
    writer.writeAttribute(XSL_ATTR_SELECT, "normalize-space(.)");
    writer.writeEndElement();

    // Done
    writer.writeEndElement();
  }

  private static void writeAttributes(XMLStreamWriter writer, Set<ElementMapping> attributeMappings)
      throws XMLStreamException {
    for (ElementMapping attribute : attributeMappings) {

      // Write comment.
      writer.writeComment(" Attribute mapping: " + attribute.toString() + " ");

      // Check if the attribute exists in the source.
      writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_IF);
      writer.writeAttribute(XSL_ATTR_TEST,
          VALUE_XSL_ATTR_PREFIX + attribute.getFrom().toString(TAG_SEPARATOR));

      // Copy the attribute.
      writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_ATTRIBUTE);
      writer.writeAttribute(XSL_ATTR_NAME, attribute.getTo().toString(TAG_SEPARATOR));
      writer.writeStartElement(Namespace.XSL.getUri(), XSL_TAG_VALUE_OF);
      writer.writeAttribute(XSL_ATTR_SELECT,
          VALUE_XSL_ATTR_PREFIX + attribute.getFrom().toString(TAG_SEPARATOR));

      // Done.
      writer.writeEndElement();
      writer.writeEndElement();
      writer.writeEndElement();
    }
  }
}
