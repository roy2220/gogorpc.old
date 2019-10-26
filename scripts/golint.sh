#!/usr/bin/env bash

set -o errexit -o nounset -o pipefail # -o xtrace

excluded_rules_file=$(dirname "$(realpath "${0}")")/golint.excluded_rules
complaints=$(golint ${*} | grep --invert-match --file "${excluded_rules_file}" || true)

if [[ ! -z "${complaints}" ]]; then
    echo "${complaints}"
    exit 1
fi
