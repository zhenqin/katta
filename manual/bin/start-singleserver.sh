#!/usr/bin/env bash

unset CDPATH
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/katta-config.sh

# start simgle server daemons
"$bin"/katta-daemons.sh start server --config $KATTA_CONF_DIR
