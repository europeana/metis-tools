package eu.europeana.metis.wiki;

import eu.europeana.corelib.definitions.edm.entity.Address;
import eu.europeana.corelib.definitions.edm.entity.Organization;
import eu.europeana.corelib.solr.entity.AddressImpl;
import eu.europeana.corelib.solr.entity.OrganizationImpl;
import eu.europeana.enrichment.api.external.model.*;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import javax.xml.bind.JAXBException;

import eu.europeana.enrichment.internal.model.OrganizationEnrichmentEntity;
import eu.europeana.metis.utils.Constants;
import eu.europeana.metis.utils.ConverterUtils;
import eu.europeana.metis.zoho.exception.WikidataAccessException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Wikidata Access Service class
 *
 * @author Srishti Singh (srishti.singh@europeana.eu)
 * @since 2021-07-06
 */
public class WikidataAccessService {
    private static final Logger LOGGER = LoggerFactory.getLogger(WikidataAccessService.class);
    private final WikidataAccessDao wikidataAccessDao;
    private final ConverterUtils converterUtils = new ConverterUtils();

    public WikidataAccessService(WikidataAccessDao wikidataAccessDao) {
        this.wikidataAccessDao = wikidataAccessDao;
    }

    public ConverterUtils getConverterUtils() {
        return this.converterUtils;
    }

    protected WikidataAccessDao getWikidataAccessDao() {
        return this.wikidataAccessDao;
    }

    public URI buildOrganizationUri(String organizationId) {
        String contactsSearchUrl = String.format("%s%s", Constants.WIKIDATA_BASE_URL, organizationId);
        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(contactsSearchUrl);
        return builder.build().encode().toUri();
    }

    public Organization dereference(String wikidataUri) throws WikidataAccessException {
        StringBuilder wikidataXml = null;
        WikidataOrganization wikidataOrganization = null;
        try {
            wikidataXml = this.getWikidataAccessDao().getEntity(wikidataUri);
            wikidataOrganization = this.getWikidataAccessDao().parse(wikidataXml.toString());
        } catch (JAXBException var5) {
            LOGGER.debug("Cannot parse wikidata response: {}", wikidataXml);
            throw new WikidataAccessException("Cannot parse wikidata xml response for uri: " + wikidataUri, var5);
        }

        return wikidataOrganization == null ? null : this.toOrganizationImpl(wikidataOrganization);
    }

    public WikidataOrganization parseWikidataOrganization(File inputFile) throws JAXBException {
        return this.wikidataAccessDao.parseWikidataOrganization(inputFile);
    }

    public Organization toOrganizationImpl(WikidataOrganization wikidataOrganization) {
        OrganizationImpl org = new OrganizationImpl();
        eu.europeana.enrichment.api.external.model.Organization edmOrganization = wikidataOrganization.getOrganization();
        String logo;
        if (edmOrganization.getAbout() != null) {
            logo = edmOrganization.getAbout();
            if (StringUtils.isNotEmpty(logo)) {
                org.setAbout(logo);
            }
        }

        if (edmOrganization.getCountry() != null) {
            logo = edmOrganization.getCountry();
            org.setEdmCountry(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), logo));
        }

        if (edmOrganization.getHomepage() != null) {
            logo = edmOrganization.getHomepage().getResource();
            org.setFoafHomepage(logo);
        }

        if (edmOrganization.getLogo() != null) {
            logo = edmOrganization.getLogo().getResource();
            org.setFoafLogo(logo);
        }

        if (edmOrganization.getDepiction() != null) {
            logo = edmOrganization.getDepiction().getResource();
            org.setFoafDepiction(logo);
        }

        if (edmOrganization.getMbox() != null) {
            logo = edmOrganization.getMbox();
            org.setFoafMbox(this.getConverterUtils().createList(logo));
        }

        if (edmOrganization.getPhone() != null) {
            logo = edmOrganization.getPhone();
            org.setFoafPhone(this.getConverterUtils().createList(logo));
        }

        if (edmOrganization.getLogo() != null) {
            logo = edmOrganization.getLogo().getResource();
            org.setFoafLogo(logo);
        }

        List<Label> acronymLabel = edmOrganization.getAcronyms();
        org.setEdmAcronym(this.getConverterUtils().createMapWithListsFromLabelList(acronymLabel));

        List<Part> sameAs = edmOrganization.getSameAs();
        org.setOwlSameAs(this.getConverterUtils().createStringArrayFromPartList(sameAs));
        List<Label> descriptions = edmOrganization.getDescriptions();
        org.setDcDescription(this.getConverterUtils().createMapFromLabelList(descriptions));

        List<Label> prefLabel = edmOrganization.getPrefLabelList();
        org.setPrefLabel(this.getConverterUtils().createMapWithListsFromLabelList(prefLabel));
        List<Label> altLabel = edmOrganization.getAltLabelList();
        org.setAltLabel(this.getConverterUtils().createMapWithListsFromLabelList(altLabel));
        if (edmOrganization.getHasAddress() != null && edmOrganization.getHasAddress().getVcardAddressesList() != null) {
            VcardAddress vcardAddress = edmOrganization.getHasAddress().getVcardAddressesList().get(0);
            Address address = new AddressImpl();
            address.setAbout(org.getAbout() + Constants.ADDRESS_ABOUT);
            address.setVcardCountryName(vcardAddress.getCountryName());
            if (vcardAddress.getHasGeo() != null) {
                address.setVcardHasGeo(vcardAddress.getHasGeo().getResource());
            }

            org.setAddress(address);
        }

        return org;
    }

    public void saveXmlToFile(String xml, File contentFile) throws WikidataAccessException {
        try {
            boolean wasFileCreated = contentFile.createNewFile();
            if (!wasFileCreated) {
                LOGGER.warn("Content file existed, it will be overwritten: {}", contentFile.getAbsolutePath());
            }
            FileUtils.write(contentFile, xml, StandardCharsets.UTF_8.name());
        } catch (IOException var4) {
            throw new WikidataAccessException("XML could not be written to a file.", var4);
        }
    }

    public void mergePropsFromWikidata(OrganizationEnrichmentEntity organizationEnrichmentEntity, Organization wikidataOrganization) {
        Map<String, List<String>> addToAltLabelMap = new HashMap();
        if (wikidataOrganization.getPrefLabel() != null) {
            Map<String, List<String>> newPrefLabelMap = this.getConverterUtils().mergeMapsWithSingletonLists(organizationEnrichmentEntity.getPrefLabel(), wikidataOrganization.getPrefLabel(), addToAltLabelMap);
            organizationEnrichmentEntity.setPrefLabel(newPrefLabelMap);
        }
        if (wikidataOrganization.getAltLabel() != null) {
            Map<String, List<String>> allWikidataAltLabels = this.getConverterUtils().mergeMapsWithLists(wikidataOrganization.getAltLabel(), addToAltLabelMap);
            Map<String, List<String>> mergedAltLabelMap = this.getConverterUtils().mergeMapsWithLists(allWikidataAltLabels, organizationEnrichmentEntity.getAltLabel());
            organizationEnrichmentEntity.setAltLabel(mergedAltLabelMap);
        }

        if (wikidataOrganization.getEdmAcronym() != null) {
            Map<String, List<String>> acronyms = this.getConverterUtils().mergeMapsWithLists(organizationEnrichmentEntity.getEdmAcronym(), wikidataOrganization.getEdmAcronym());
            organizationEnrichmentEntity.setEdmAcronym(acronyms);
        }
        if (StringUtils.isEmpty(organizationEnrichmentEntity.getFoafLogo())) {
            organizationEnrichmentEntity.setFoafLogo(wikidataOrganization.getFoafLogo());
        }
        if (StringUtils.isEmpty(organizationEnrichmentEntity.getFoafDepiction())) {
            organizationEnrichmentEntity.setFoafDepiction(wikidataOrganization.getFoafDepiction());
        }
        if (StringUtils.isEmpty(organizationEnrichmentEntity.getFoafHomepage())) {
            organizationEnrichmentEntity.setFoafLogo(wikidataOrganization.getFoafLogo());
        }
        List<String> phoneList = this.getConverterUtils().mergeStringLists(organizationEnrichmentEntity.getFoafPhone(), wikidataOrganization.getFoafPhone());
        organizationEnrichmentEntity.setFoafPhone(phoneList);
        List<String> mbox = this.getConverterUtils().mergeStringLists(organizationEnrichmentEntity.getFoafMbox(), wikidataOrganization.getFoafMbox());
        organizationEnrichmentEntity.setFoafMbox(mbox);
        List<String> sameAs = this.buildSameAs(organizationEnrichmentEntity, wikidataOrganization);
        organizationEnrichmentEntity.setOwlSameAs(sameAs);
        organizationEnrichmentEntity.setDcDescription(wikidataOrganization.getDcDescription());
        this.getConverterUtils().mergeAddress(organizationEnrichmentEntity, wikidataOrganization);
    }

    private List<String> buildSameAs(OrganizationEnrichmentEntity organizationEnrichmentEntity, Organization wikidataOrganization) {
        List<String> mergedSameAs = organizationEnrichmentEntity.getOwlSameAs();
        for (int i = 0; i < wikidataOrganization.getOwlSameAs().length; i++) {
          mergedSameAs.add(wikidataOrganization.getOwlSameAs()[i]);
        }
        String wikidataResourceUri = wikidataOrganization.getAbout();
        if (!mergedSameAs.contains(wikidataResourceUri)) {
            mergedSameAs.add(wikidataResourceUri);
        }
        return mergedSameAs;
    }
}
