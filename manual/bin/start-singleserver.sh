#!/usr/bin/env bash

unset CDPATH
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/katta-config.sh

if [ -f "${KATTA_CONF_DIR}/katta-env.sh" ]; then
  . "${KATTA_CONF_DIR}/katta-env.sh"
fi

# start simgle server daemons
"$bin"/katta-daemon.sh start server --config $KATTA_CONF_DIR
