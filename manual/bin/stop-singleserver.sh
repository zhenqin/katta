#!/usr/bin/env bash

# Stop all katta daemons.  Run this on master node.

unset CDPATH
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/katta-config.sh

if [ -f "${KATTA_CONF_DIR}/katta-env.sh" ]; then
  . "${KATTA_CONF_DIR}/katta-env.sh"
fi

# stop node daemons
"$bin"/katta-daemon.sh stop server --config $KATTA_CONF_DIR
