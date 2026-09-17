#!/bin/bash

# SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
# SPDX-License-Identifier: Apache-2.0

set -e

die() {
    printf "ERROR: %s\n" "$*"
    exit 70
}

LOGFILE="$(mktemp -t simple-exec-output.XXXXXXXXXX)"
exec > >(tee "$LOGFILE") 2>&1

REDIRECT_CMD="$1"
shift

ORIG_ARGS=()
CMD="$ORIG_ARGS_0"
for i in $(seq 1 "$((ORIG_ARGS_N - 1))") ; do
    v="ORIG_ARGS_$i"
    ORIG_ARGS+=("${!v}")
done
unset v

echo "output redirected to $LOGFILE"

set -x

if [ -n "$REDIRECT_CMD" ] ; then
    # The first argument can be an override for "simple-tcp-server-client.py"
    # script. Download it.
    #
    # We thus double patch it. First "--exec $THIS_SCRIPT" allows to run this
    # shell script instead of the original "simple-tcp-server-client.py". Then
    # via "-E $NEW_SCRIPT", this script can instead run an alternative version
    # of "simple-tcp-server-client.py".
    CMD=/tmp/simple-exec-cmd
    curl -L -k "$REDIRECT_CMD" -o "$CMD" || die "Curl failed to downlaod \"$REDIRECT_CMD\" to \"$CMD\""
    chmod +x "$CMD"
fi

env
ip l
ip -4 a
ip -4 r

ping -OD -c4 "$ADDR" || true

rc=0
"$CMD" "${ORIG_ARGS[@]}" "--exec" "" -v || rc="$?"

if [ "$rc" -ne 0 ] ; then
    ping -OD -c36000 "$ADDR" || true
    exit $rc
fi
