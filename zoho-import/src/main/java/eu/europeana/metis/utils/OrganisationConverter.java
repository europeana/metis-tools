package eu.europeana.metis.utils;

import com.zoho.crm.api.record.Record;
import eu.europeana.enrichment.internal.model.Address;
import eu.europeana.enrichment.internal.model.OrganizationEnrichmentEntity;
import eu.europeana.metis.zoho.ZohoConstants;
import eu.europeana.metis.zoho.ZohoUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Locale;

import static eu.europeana.metis.utils.ConverterUtils.toIsoLanguage;

/**
 * Converter for Organisation Importer Class
 *
 * @author Srishti Singh (srishti.singh@europeana.eu)
 * @since 2021-07-06
 */
public class OrganisationConverter {

    private ConverterUtils converterUtils = new ConverterUtils();

    public ConverterUtils getConverterUtils() {
        return this.converterUtils;
    }

    /**
     * Converts Zoho Record into OrganizationEnrichmentEntity
     * @param record
     * @return org
     */
    public OrganizationEnrichmentEntity convertToOrganizationEnrichmentEntity(Record record) {
        OrganizationEnrichmentEntity org = new OrganizationEnrichmentEntity();
        org.setAbout(Constants.URL_ORGANIZATION_PREFFIX + Long.toString(record.getId()));
        org.setDcIdentifier(this.getConverterUtils().createMapWithLists(Constants.UNDEFINED_LANGUAGE_KEY, Long.toString(record.getId())));
        String isoLanguage = toIsoLanguage(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.LANG_ORGANIZATION_NAME_FIELD)));
        org.setPrefLabel(this.getConverterUtils().createMapWithLists(isoLanguage, ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.ACCOUNT_NAME_FIELD))));
        String isoLanguage1 = toIsoLanguage(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.LANG_ALTERNATIVE_FIELD)));
        org.setAltLabel(this.getConverterUtils().createMapWithLists(isoLanguage1, ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.ALTERNATIVE_FIELD))));

        String acronym = ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.ACRONYM_FIELD));
        String langAcronym = ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.LANG_ACRONYM_FIELD));
        org.setEdmAcronym(this.getConverterUtils().createLanguageMapOfStringList(langAcronym, acronym));
        org.setFoafLogo(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.LOGO_LINK_TO_WIKIMEDIACOMMONS_FIELD)));
        org.setFoafHomepage(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.WEBSITE_FIELD)));
        List<String> organizationRoleStringList = ZohoUtils.stringListSupplier(record.getKeyValue(ZohoConstants.ORGANIZATION_ROLE_FIELD));
        if (!organizationRoleStringList.isEmpty()) {
            org.setEdmEuropeanaRole(this.getConverterUtils().createLanguageMapOfStringList(Locale.ENGLISH.getLanguage(), organizationRoleStringList));
        }
        org.setEdmOrganizationDomain(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.DOMAIN_FIELD))));
        org.setEdmOrganizationSector(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.SECTOR_FIELD))));
        org.setEdmOrganizationScope(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.SCOPE_FIELD))));
        org.setEdmGeorgraphicLevel(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.GEOGRAPHIC_LEVEL_FIELD))));
        String organizationCountry = this.toEdmCountry(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.ORGANIZATION_COUNTRY_FIELD)));
        org.setEdmCountry(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), organizationCountry));
        List<String> sameAs = ZohoUtils.stringListSupplier(record.getKeyValue(ZohoConstants.SAME_AS_FIELD));
        if (!sameAs.isEmpty()) {
            org.setOwlSameAs(sameAs);

        }
        Address address = new Address();
        address.setAbout(org.getAbout() + Constants.ADDRESS_ABOUT);
        address.setVcardStreetAddress(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.STREET_FIELD)));
        address.setVcardLocality(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.CITY_FIELD)));
        address.setVcardCountryName(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.ADDRESS_COUNTRY_FIELD)));
        address.setVcardPostalCode(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.ZIP_CODE_FIELD)));
        address.setVcardPostOfficeBox(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.PO_BOX_FIELD)));
        org.setAddress(address);

        return org;
    }

    /**
     * Returns the isoCode for the organizationCountry
     * @param organizationCountry
     * @return
     */
    String toEdmCountry(String organizationCountry) {
        if (StringUtils.isBlank(organizationCountry)) {
            return null;
        } else {
            String isoCode = null;
            int commaSeparatorPos = organizationCountry.indexOf(44);
            int bracketSeparatorPos = organizationCountry.indexOf(40);
            if (commaSeparatorPos > 0) {
                isoCode = organizationCountry.substring(commaSeparatorPos + 1).trim();
            } else if (bracketSeparatorPos > 0) {
                isoCode = organizationCountry.substring(0, bracketSeparatorPos).trim();
            }

            return isoCode;
        }
    }
}
