package eu.europeana.metis_tools.oaipmh.harvest;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * Looks at a OAI-PMH endpoint and harvest the one record that we need.
 */
public class OaiPmhRecordHarvestMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(OaiPmhRecordHarvestMain.class);

  private static final String OAI_PMH_NAME_SPACE = "http://www.openarchives.org/OAI/2.0/";

  public static void main(String[] args) throws SAXException, InterruptedException, IOException,
          XPathExpressionException, URISyntaxException, ParserConfigurationException, TransformerException {

    // DEFINE THE INPUT HERE
    final String metisRecordToFind = "/2059211/dyn_portal_index_seam_page_alo_aloId_11785";
    final OaiPmhService service = new OaiPmhService(
            "http://panic.image.ece.ntua.gr:9876/sounds/oai", "1019", "rdf");

    // Create the XML document builder
    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    final DocumentBuilder builder = factory.newDocumentBuilder();

    // Determine the actual pattern to search
    final Pattern regex = Pattern
            .compile(metisRecordToFind.split("/")[2].replace("_", ".") + "\\Z");

    // Search for the record.
    final Element element = harvest(service, regex, builder);

    // If we find the record, we print.
    if (element != null) {
      final Transformer transformer = TransformerFactory.newInstance().newTransformer();
      final StringWriter writer = new StringWriter();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
      transformer.transform(new DOMSource(element), new StreamResult(writer));
      final String result = writer.getBuffer().toString();
      System.out.println(result);
    } else {
      LOGGER.warn("Record not found!");
    }
  }

  private static Element harvest(OaiPmhService service, Pattern rdfAboutMatcher,
          DocumentBuilder builder)
          throws InterruptedException, IOException, SAXException, URISyntaxException, XPathExpressionException {

    // Create the xpath expression that resolves the rdf:about from a record (so starting in the current record).
    final XPathExpression rdfAboutXpath = XPathFactory.newInstance().newXPath().compile(
            ".//*[local-name()='ProvidedCHO' and namespace-uri()='http://www.europeana.eu/schemas/edm/']"
                    + "/@*[local-name()='about' and namespace-uri()='http://www.w3.org/1999/02/22-rdf-syntax-ns#']");

    // Harvest the records in batches
    String resumptionToken = null;
    while (true) {

      // Harvest the next batch
      final Document batch = harvestBatch(service, resumptionToken, builder);

      // Get the record list.
      final Element listRecords = (Element) batch.getDocumentElement()
              .getElementsByTagNameNS(OAI_PMH_NAME_SPACE, "ListRecords").item(0);
      if (listRecords == null) {
        break;
      }
      final NodeList recordList = listRecords.getElementsByTagNameNS(OAI_PMH_NAME_SPACE, "record");

      // Check the records
      for (int recordIndex = 0; recordIndex < recordList.getLength(); recordIndex++) {
        final Element record = (Element) recordList.item(recordIndex);
        final String rdfAbout = rdfAboutXpath.evaluate(record);
        if (rdfAboutMatcher.matcher(rdfAbout).find()) {
          return record;
        }
      }

      // So the record is not found. Get the resumption token if any.
      final NodeList resumptionElementList = listRecords
              .getElementsByTagNameNS(OAI_PMH_NAME_SPACE, "resumptionToken");
      if (resumptionElementList.getLength() > 0) {
        final Element resumptionElement = (Element) resumptionElementList.item(0);
        final String cursor = resumptionElement.getAttributeNS(null, "cursor");
        final String completeListSize = resumptionElement.getAttributeNS(null, "completeListSize");
        LOGGER.info("Processed {} of {} records.", cursor, completeListSize);
        resumptionToken = resumptionElement.getTextContent();
      } else {
        resumptionToken = null;
        LOGGER.info("Processed final {} records.", recordList.getLength());
      }

      // Check if there is more to harvest
      if (resumptionToken == null) {
        break;
      }
    }

    // So: we could not find the record.
    return null;
  }

  private static Document harvestBatch(OaiPmhService service, String resumptionToken,
          DocumentBuilder builder)
          throws URISyntaxException, IOException, InterruptedException, SAXException {

    // Compute the URL.
    final Map<String, String> urlParameters = new HashMap<>();
    urlParameters.put("verb", "ListRecords");
    if (resumptionToken == null) {
      urlParameters.put("set", service.getOaiPmhSetSpec());
      urlParameters.put("metadataPrefix", service.getOaiPmhMetadataFormat());
    } else {
      urlParameters.put("resumptionToken", resumptionToken);
    }
    final String query = urlParameters.entrySet().stream().map(entry ->
            entry.getKey() + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8)
    ).collect(Collectors.joining("&", "?", ""));
    final URI uri = new URI(service.getOaiPmhEndpoint() + query);
    LOGGER.info("Calling URL: {}", uri);

    // Send the request
    final HttpClient client = HttpClient.newBuilder().build();
    final HttpRequest request = HttpRequest.newBuilder().uri(uri)
            .header("Accept", "application/rdf+xml").build();
    final HttpResponse<InputStream> response = client.send(request, BodyHandlers.ofInputStream());

    // Convert to xml document
    try (final InputStream inputStream = response.body()) {
      return builder.parse(inputStream);
    }
  }
}
