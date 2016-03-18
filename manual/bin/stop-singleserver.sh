#!/usr/bin/env bash

# Stop all katta daemons.  Run this on master node.

unset CDPATH
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/katta-config.sh

# stop node daemons
"$bin"/katta-daemons.sh stop server --config $KATTA_CONF_DIR
