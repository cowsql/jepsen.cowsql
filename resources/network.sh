#!/bin/sh -e

BRIDGE="jepsen-br"
HOSTS="${HOSTS:-/etc/hosts}"

help() {
    echo "Set up and tear down network resources for inter-node communication."
    echo
    echo "Usage:"
    echo
    echo "$0 setup <n of nodes>"
    echo "$0 teardown <n of nodes>"
}

if [ "${#}" -lt 2 ]; then
    help
    exit 1
fi

cmd="${1}"
n="${2}"

if [ "${cmd}" = "setup" ]; then
    ip link add name "${BRIDGE}" type bridge
    ip link set "${BRIDGE}" up
    ip addr add 10.2.1.1/24 brd + dev "${BRIDGE}"
    for i in $(seq "${n}"); do
        if ! grep -qE "^10.2.1.1${i} n${i}" "${HOSTS}"; then
            echo "10.2.1.1${i} n${i}" >> "${HOSTS}"
        fi
    done
    exit 0
fi

if [ "${cmd}" = "teardown" ]; then
    ip link del "${BRIDGE}"
    for i in $(seq 5); do
        sed -i "/^10.2.1.1${i} n${i}/d" "${HOSTS}"
    done
    exit 0
fi

help
exit 1
