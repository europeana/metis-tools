package eu.europeana.metis.enrichment.utilities;

import eu.europeana.enrichment.api.external.model.LabelInfo;
import eu.europeana.enrichment.internal.model.AbstractEnrichmentEntity;
import eu.europeana.enrichment.internal.model.EnrichmentTerm;
import eu.europeana.enrichment.internal.model.TimespanEnrichmentEntity;
import eu.europeana.enrichment.utils.EntityType;
import eu.europeana.normalization.util.Namespace;
import eu.europeana.normalization.util.XmlException;
import eu.europeana.normalization.util.XmlUtil;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class EnrichmentTermUtils {

  private static final String OWL_NAMESPACE = "http://www.w3.org/2002/07/owl#";

  private EnrichmentTermUtils() {
  }

  public static List<EnrichmentTerm> getTimespansFromDocument(String timespanFileWithXmls)
      throws IOException, XmlException {
    final Document document = XmlUtil.parseDom(new FileReader(new File(timespanFileWithXmls)));

    final List<Element> timeSpanElements = XmlUtil
        .getAsElementList(document.getElementsByTagNameNS(Namespace.EDM.getUri(), "TimeSpan"));
    List<EnrichmentTerm> enrichmentTerms = new ArrayList<>();
    for (Element timeSpan : timeSpanElements) {

      final String rdfAbout = timeSpan.getAttributeNS(Namespace.RDF.getUri(), "about");
      enrichmentTerms.add(createEnrichmentEntity(timeSpan, rdfAbout));
    }
    return enrichmentTerms;
  }

  public static EnrichmentTerm createEnrichmentEntity(Element timeSpan, String rdfAbout) {

    final TimespanEnrichmentEntity timespanEnrichmentEntity = new TimespanEnrichmentEntity();
    timespanEnrichmentEntity.setAbout(rdfAbout);

    //prefLabel
    final List<Element> prefLabelElements = XmlUtil
        .getAsElementList(timeSpan.getElementsByTagNameNS(Namespace.SKOS.getUri(), "prefLabel"));
    timespanEnrichmentEntity.setPrefLabel(getMapFromElementsWithLanguage(prefLabelElements));

    //altLabel
    final List<Element> altLabelElements = XmlUtil
        .getAsElementList(timeSpan.getElementsByTagNameNS(Namespace.SKOS.getUri(), "altLabel"));
    timespanEnrichmentEntity.setAltLabel(getMapFromElementsWithLanguage(altLabelElements));

    //hiddenLabel
    final List<Element> hiddenLabelElements = XmlUtil
        .getAsElementList(timeSpan.getElementsByTagNameNS(Namespace.SKOS.getUri(), "hiddenLabel"));
    timespanEnrichmentEntity.setHiddenLabel(getMapFromElementsWithLanguage(hiddenLabelElements));

    //begin
    final List<Element> beginElements = XmlUtil
        .getAsElementList(timeSpan.getElementsByTagNameNS(Namespace.EDM.getUri(), "begin"));
    timespanEnrichmentEntity.setBegin(getMapFromElementsWithLanguage(beginElements));

    //end
    final List<Element> endElements = XmlUtil
        .getAsElementList(timeSpan.getElementsByTagNameNS(Namespace.EDM.getUri(), "end"));
    timespanEnrichmentEntity.setEnd(getMapFromElementsWithLanguage(endElements));

    //isPartOf
    final List<Element> isPartOfElements = XmlUtil
        .getAsElementList(timeSpan.getElementsByTagNameNS(Namespace.DCTERMS.getUri(), "isPartOf"));
    timespanEnrichmentEntity.setIsPartOf(getMapFromElementsWithResource(isPartOfElements));

    //isNextInSequence
    final List<Element> isNextInSequenceList = XmlUtil.getAsElementList(
        timeSpan.getElementsByTagNameNS(Namespace.EDM.getUri(), "isNextInSequence"));
    final String[] isNextInSequence = getListFromElements(isNextInSequenceList)
        .toArray(new String[0]);
    timespanEnrichmentEntity
        .setIsNextInSequence(isNextInSequence.length == 0 ? null : isNextInSequence[0]);

    //sameAs
    final List<Element> sameAsList = XmlUtil
        .getAsElementList(timeSpan.getElementsByTagNameNS(OWL_NAMESPACE, "sameAs"));
    final List<String> owlSameAs = getListFromElements(sameAsList);
    timespanEnrichmentEntity.setOwlSameAs(owlSameAs);

    final EnrichmentTerm enrichmentTerm = new EnrichmentTerm();
    enrichmentTerm.setEntityType(EntityType.TIMESPAN);
    enrichmentTerm.setParent(timespanEnrichmentEntity.getIsPartOf().get("def").get(0));
    enrichmentTerm.setEnrichmentEntity(timespanEnrichmentEntity);
    enrichmentTerm.setLabelInfos(createLabelInfoList(timespanEnrichmentEntity));

    return enrichmentTerm;
  }

  private static HashMap<String, List<String>> getMapFromElementsWithLanguage(
      List<Element> elements) {
    final HashMap<String, List<String>> languageTextMap = new HashMap<>();
    for (Element element : elements) {
      final String language = Optional
          .ofNullable(element.getAttributeNodeNS(Namespace.XML.getUri(), "lang"))
          .map(Attr::getValue).orElse("def");
      final String textContent = element.getTextContent();
      final List<String> textList = languageTextMap
          .computeIfAbsent(language, lang -> new ArrayList<>());
      textList.add(textContent);
    }
    return languageTextMap;
  }

  private static HashMap<String, List<String>> getMapFromElementsWithResource(
      List<Element> elements) {
    final HashMap<String, List<String>> resourceMap = new HashMap<>();
    if (elements.size() > 0) {
      final String resource = elements.get(0).getAttributeNodeNS(Namespace.RDF.getUri(), "resource")
          .getValue();
      resourceMap.put("def", List.of(resource));
    }
    return resourceMap;

  }

  private static List<String> getListFromElements(List<Element> elements) {
    List<String> resources = new ArrayList<>();
    for (Element element : elements) {
      final String resource = element.getAttributeNodeNS(Namespace.RDF.getUri(), "resource")
          .getValue();
      if (StringUtils.isNotBlank(resource)) {
        resources.add(resource);
      }
    }
    return resources;
  }

  public static List<LabelInfo> createLabelInfoList(
      AbstractEnrichmentEntity abstractEnrichmentEntity) {
    final Map<String, List<String>> combinedLabels = new HashMap<>();
    final Map<String, List<String>> prefLabel = abstractEnrichmentEntity.getPrefLabel();
    final Map<String, List<String>> altLabel = abstractEnrichmentEntity.getAltLabel();

    copyToCombinedLabels(combinedLabels, prefLabel);
    copyToCombinedLabels(combinedLabels, altLabel);

    return combinedLabels.entrySet().stream()
        .map(entry -> new LabelInfo(entry.getValue(), entry.getKey())).collect(Collectors.toList());
  }

  private static void copyToCombinedLabels(Map<String, List<String>> combinedLabels,
      Map<String, List<String>> prefLabel) {
    if (prefLabel != null) {
      prefLabel.forEach((key, value) -> {
        value = value.stream().map(String::toLowerCase).collect(Collectors.toList());
        combinedLabels.merge(key, value,
            (v1, v2) -> Stream.of(v1, v2).flatMap(List::stream).distinct()
                .collect(Collectors.toList()));
      });
    }
  }

  //  public static List<String> createOwlSameAsList(EnrichmentTerm enrichmentTerm) {
  //    final Stream<String> owlSameAsInTerm = Optional.ofNullable(enrichmentTerm.getOwlSameAs())
  //        .stream().flatMap(Collection::stream);
  //    final AbstractEnrichmentEntity enrichmentEntity = enrichmentTerm.getEnrichmentEntity();
  //    final Stream<String> owlSameAsInEntity = Optional.ofNullable(enrichmentEntity.getOwlSameAs())
  //        .stream().flatMap(Collection::stream);
  //    return Stream.concat(owlSameAsInTerm, owlSameAsInEntity).distinct()
  //        .collect(Collectors.toList());
  //  }

}
