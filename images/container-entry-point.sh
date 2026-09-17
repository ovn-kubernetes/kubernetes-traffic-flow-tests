#!/bin/bash

# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

if [ "$#" -eq 0 ] ; then
    ARGS=("/usr/bin/sleep" "infinity")
else
    ARGS=("$@")
fi

exec /usr/bin/tini -p SIGTERM -g -e 143 -- "${ARGS[@]}"
