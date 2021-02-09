#!/usr/bin/env bash
# Check how many OAI repositories used in Metis support deleted/persistent/transient tag
PATH_TO_FILE_WITH_URLS=$1

NO_PATTERN="<deletedRecord>no</deletedRecord>"
PERSISTENT_PATTERN="<deletedRecord>persistent</deletedRecord>"
TRANSIENT_PATTERN="<deletedRecord>transient</deletedRecord>"

RESPONSE=$(cat ${PATH_TO_FILE_WITH_URLS} | xargs printf '%s?verb=Identify\n' | xargs curl -m 30)

REPOSITORIES_NO_NO=$(echo ${RESPONSE} | grep -o ${NO_PATTERN} | wc -l)
REPOSITORIES_PERSISTENT_NO=$(echo ${RESPONSE} | grep -o ${PERSISTENT_PATTERN} | wc -l)
REPOSITORIES_TRANSIENT_NO=$(echo ${RESPONSE} | grep -o ${TRANSIENT_PATTERN} | wc -l)

echo ${NO_PATTERN} ${REPOSITORIES_NO_NO}
echo ${PERSISTENT_PATTERN} ${REPOSITORIES_PERSISTENT_NO}
echo ${TRANSIENT_PATTERN} ${REPOSITORIES_TRANSIENT_NO}
