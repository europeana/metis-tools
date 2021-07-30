package eu.europeana.metis.utils;

import eu.europeana.corelib.definitions.edm.entity.Organization;

import eu.europeana.enrichment.api.external.model.Label;
import eu.europeana.enrichment.api.external.model.WebResource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import eu.europeana.enrichment.internal.model.Address;
import eu.europeana.enrichment.internal.model.OrganizationEnrichmentEntity;
import io.github.classgraph.AnnotationParameterValueList;

import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for Organisation Importer
 *
 * @author Srishti Singh (srishti.singh@europeana.eu)
 * @since 2021-07-06
 */
public class ConverterUtils {

    public ConverterUtils() {
        //
    }

    /**
     * creates a Map with list of key and value
     * @param key
     * @param value
     * @return
     */
    public Map<String, List<String>> createMapWithLists(String key, String value) {
        return value == null ? null : Collections.singletonMap(key, Collections.singletonList(value));
    }

    /**
     * Creates a map of key and value
     * @param key
     * @param value
     * @return
     */
    public Map<String, String> createMap(String key, String value) {
        return StringUtils.isBlank(value) ? null : Collections.singletonMap(key, value);
    }

    /**
     * Creates a List with value
     * @param value
     * @return
     */
    public List<String> createList(String value) {
        return value == null ? null : Collections.singletonList(value);
    }

    /**
     * Creates a Map with List for List < ? extends Label>
     * @param labels
     * @return
     */
    public Map<String, List<String>> createMapWithListsFromLabelList(List<Label> labels) {
        if (labels != null && !labels.isEmpty()) {
            Map<String, List<String>> resMap = new HashMap();
            String lang;
            for(Label label : labels) {
            	lang = toIsoLanguage(label.getLang());
            	if(resMap.containsKey(lang)) {
            		resMap.get(lang).add(label.getValue());
            	} else {
            		ArrayList<String> valueList = new ArrayList<String>();
            		valueList.add(label.getValue());
					resMap.put(lang, valueList);
            	}
            }
            return resMap;
        }
        return null;
    }

    /**
     * Creates a map for List < ? extends Label>
     * @param labels
     * @return
     */
    public Map<String, String> createMapFromLabelList(List<Label> labels) {
        if (labels != null && !labels.isEmpty()) {
            Map<String, String> resMap = new HashMap();
            for(Label label : labels) {
                resMap.put(toIsoLanguage(label.getLang()), label.getValue());
            }
            return resMap;
        }
        return null;
    }

    /**
     * Creates a String array for List < ? extends Part>
     * @param resources
     * @return
     */
    public String[] createStringArrayFromPartList(List<? extends WebResource> resources) {
        return resources == null ? null : resources.stream().map(WebResource::getResourceUri).toArray((x$0) -> {
            return new String[x$0];
        });
    }

    /**
     * create language map of List
     * @param language
     * @param value
     * @return
     */
    public Map<String, List<String>> createLanguageMapOfStringList(String language, String value) {
        return value == null ? null : Collections.singletonMap(toIsoLanguage(language), this.createList(value));
    }

    /**
     * create language map of list from list of values
     * @param language
     * @param value
     * @return
     */
    public Map<String, List<String>> createLanguageMapOfStringList(String language, List<String> value) {
        return value == null ? null : Collections.singletonMap(toIsoLanguage(language), value);
    }

    /**
     * converts language into ISO language.
     * Defaults it to "def" if empty
     *
     * @param language
     * @return
     */
    public static String toIsoLanguage(String language) {
        return StringUtils.isBlank(language) ? Constants.UNDEFINED_LANGUAGE_KEY : language.substring(0, 2).toLowerCase(Locale.US);
    }

    /**
     * Merges maps with list of values
     *
     * @param baseMap
     * @param addMap
     * @return
     */
    public Map<String, List<String>> mergeMapsWithLists(Map<String, List<String>> baseMap, Map<String, List<String>> addMap) {
        if (baseMap == null && addMap == null) {
            return null;
        } else {
            Map<String, List<String>> result = new HashMap();
            if (baseMap != null) {
                result.putAll(baseMap);
            }
            if (addMap != null) {
                Iterator var4 = addMap.entrySet().iterator();

                while(var4.hasNext()) {
                    Entry<String, List<String>> entry = (Entry)var4.next();
                    result.merge(entry.getKey(), new ArrayList((Collection)entry.getValue()), this::mergeStringLists);
                }
            }
            return result.isEmpty() ? null : result;
        }
    }

    /**
     * merges maps with list of values
     * @param baseMap
     * @param addMap
     * @param notMergedMap
     * @return
     */
    public Map<String, List<String>> mergeMapsWithSingletonLists(Map<String, List<String>> baseMap, Map<String, List<String>> addMap, Map<String, List<String>> notMergedMap) {
        Map<String, List<String>> result = new HashMap(baseMap);
        Iterator iterator = addMap.entrySet().iterator();

        while(iterator.hasNext()) {
            Entry<String, List<String>> entry = (Entry)iterator.next();
            String key = entry.getKey();
            if (result.containsKey(key)) {
                List<String> unmergedValues = (List)((List)entry.getValue()).stream().distinct().filter(value -> {
                    return !((List)result.get(key)).contains(value);
                }).collect(Collectors.toList());
                if (!unmergedValues.isEmpty()) {
                    notMergedMap.merge(key, unmergedValues, this::mergeStringLists);
                }
            } else {
                result.put(key, new ArrayList(entry.getValue()));
            }
        }

        return result;
    }

    /**
     * merges lists of String
     *
     * @param baseList
     * @param addList
     * @return
     */
    public List<String> mergeStringLists(List<String> baseList, List<String> addList) {
        Set<String> result = new HashSet();
        if (baseList != null) {
            result.addAll(baseList);
        }
        if (addList != null) {
            result.addAll(addList);
        }
        return result.isEmpty() ? null : new ArrayList(result);
    }

    /**
     * Merges address
     * @param baseOrganization
     * @param addOrganization
     */
    public void mergeAddress(OrganizationEnrichmentEntity baseOrganization, Organization addOrganization) {
        if (addOrganization.getAddress() != null) {
            if (baseOrganization.getAddress() == null) {
                baseOrganization.setAddress(new Address());
            }

            Address baseAddress = baseOrganization.getAddress();
            eu.europeana.corelib.definitions.edm.entity.Address addAddress = addOrganization.getAddress();
            if (StringUtils.isEmpty(baseAddress.getVcardCountryName()) && StringUtils.isNotEmpty(addAddress.getVcardCountryName())) {
                baseAddress.setVcardCountryName(addAddress.getVcardCountryName());
            }

            if (StringUtils.isEmpty(baseAddress.getVcardHasGeo())) {
                baseAddress.setVcardHasGeo(addAddress.getVcardHasGeo());
            }

        }
    }

    /**
     * Creates a map with List of list of keys and values
     * @param keys
     * @param values
     * @return
     */
    public Map<String, List<String>> createMapWithLists(List<String> keys, List<String> values) {
        if (keys != null && !keys.isEmpty()) {
            Map<String, List<String>> resMap = new HashMap(keys.size());
            for(int i = 0; i < keys.size(); ++i) {
                resMap.put(toIsoLanguage(keys.get(i)), this.createList(values.get(i)));
            }
            return resMap;
        } else {
            return null;
        }
    }

    /**
     * merges String arrays
     * @param base
     * @param add
     * @return
     */
    public String[] mergeStringArrays(String[] base, String[] add) {
        List<String> baseList = base == null ? Collections.emptyList() : Arrays.asList(base);
        List<String> addList = add == null ? Collections.emptyList() : Arrays.asList(add);
        List<String> mergedList = this.mergeStringLists(baseList, addList);
        return mergedList == null ? null : mergedList.toArray(new String[0]);
    }
}
