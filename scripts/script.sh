#!/usr/bin/env bash
# Check how many OAI repositories used in Metis support Deleted tag
set -e
PATH_TO_FILE_WITH_URLS=$1
xargs -r < ${PATH_TO_FILE_WITH_URLS} curl | grep -o "<deletedRecord>no</deletedRecord>" | wc -l > supported-repositories-no.txt