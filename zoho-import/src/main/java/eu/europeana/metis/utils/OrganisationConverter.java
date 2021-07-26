package eu.europeana.metis.utils;

import com.zoho.crm.api.record.Record;
import eu.europeana.enrichment.internal.model.Address;
import eu.europeana.enrichment.internal.model.OrganizationEnrichmentEntity;
import eu.europeana.metis.zoho.ZohoConstants;
import eu.europeana.metis.zoho.ZohoUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

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
        // get all the alternatives labels
        org.setAltLabel(getAllAltLabel(record));

        String acronym = ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.ACRONYM_FIELD));
        String langAcronym = ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.LANG_ACRONYM_FIELD));
        org.setEdmAcronym(this.getConverterUtils().createLanguageMapOfStringList(langAcronym, acronym));
        org.setFoafLogo(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.LOGO_LINK_TO_WIKIMEDIACOMMONS_FIELD)));
        org.setFoafHomepage(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.WEBSITE_FIELD)));
        List<String> organizationRoleStringList = getZohoOrganisations(record);
        if (!organizationRoleStringList.isEmpty()) {
            org.setEdmEuropeanaRole(this.getConverterUtils().createLanguageMapOfStringList(Locale.ENGLISH.getLanguage(), organizationRoleStringList));
        }
        org.setEdmOrganizationDomain(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.DOMAIN_FIELD))));
        org.setEdmOrganizationSector(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.SECTOR_FIELD))));
        org.setEdmOrganizationScope(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.SCOPE_FIELD))));
        org.setEdmGeorgraphicLevel(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.GEOGRAPHIC_LEVEL_FIELD))));
        String organizationCountry = this.toEdmCountry(ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.ORGANIZATION_COUNTRY_FIELD)));
        org.setEdmCountry(this.getConverterUtils().createMap(Locale.ENGLISH.getLanguage(), organizationCountry));
        // get all the same As values
        List<String> sameAs = getAllSameAs(record);
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
     * Collects all the sameAs values
     * Fields : SameAs_1, SameAs_2
     *
     * @param record
     * @return
     */
    private List<String> getAllSameAs(Record record) {
        List<String> sameAsList = new ArrayList<>();
        for(int i = 0; i < Constants.SAME_AS_CODE_LENGTH; i++) {
            String sameAs = ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.SAME_AS_FIELD + "_" + i));
            if (sameAs != null) {
                sameAsList.add(sameAs);
            }
        }
        return sameAsList;
    }
    /**
     * Collects all the alternate labels values.
     * Value of Fields: "Alternative_1", "Alternative_2", ... , "Alternative_5".
     * lang value Fields: "Lang_Alternative_1", "Lang_Alternative_2", ... , "Lang_Alternative_5"
     *
     * @param record
     * @return
     */
    private Map<String, List<String>> getAllAltLabel(Record record) {
        Map<String, List<String>> altLabelMap = new HashMap<>();
        for(int i = 0; i < Constants.LANGUAGE_CODE_LENGTH; i++) {
            String label = ZohoUtils.stringFieldSupplier(record.getKeyValue(ZohoConstants.ALTERNATIVE_FIELD+ "_" + i));
            if (label != null) {
                String lang = ZohoConstants.LANG_ALTERNATIVE_FIELD + "_" + i;
                String isoLanguage = toIsoLanguage(ZohoUtils.stringFieldSupplier(record.getKeyValue(lang)));
                 altLabelMap.put(isoLanguage, Collections.singletonList(label));
            }
        }
        return altLabelMap;
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

    /**
     * The method is to process the ORGANIZATION_ROLE_FIELD values
     *
     * ZOHO Bug: ORGANIZATION_ROLE_FIELD value is comma separated string but not List of strings.
     * Hence ZohoUtils.stringFieldSupplier is used instead of ZohoUtils.stringListSupplier (as this return empty list all the time)
     * Also the value is wrapped around [] brackets.
     *
     * @param recordOrganization
     * @return
     */
    public List<String> getZohoOrganisations(Record recordOrganization) {
        List<String> organisationRolesList = new ArrayList<>();
        String zohoOrganizationRole = ZohoUtils.stringFieldSupplier(
                recordOrganization.getKeyValue(ZohoConstants.ORGANIZATION_ROLE_FIELD));
        if (zohoOrganizationRole != null) {
            // clean the string
            zohoOrganizationRole = zohoOrganizationRole.replace("[", "").replace("]", "");
            String [] orgRoles = zohoOrganizationRole.split(ZohoConstants.DELIMITER_COMMA);
            for (int i = 0; i < orgRoles.length; i++) {
                organisationRolesList.add(orgRoles[i].trim());
            }
        }
        return organisationRolesList;
    }
}
