#!/bin/sh
# Grants the monitoring user the IBM MQ authorities this tool needs. Run it on the queue manager host.
# Usage: setmqaut-monitor.sh <QMGR> <USER | -g GROUP>
#   setmqaut-monitor.sh QM1 mqmonitor
#   setmqaut-monitor.sh QM1 -g mqmonitors
# The grants are read-only, plus one put to the command queue for PCF inquiries.
set -eu

if [ "$#" -ne 2 ] && [ "$#" -ne 3 ]; then
  echo "usage: $0 <QMGR> <USER | -g GROUP>" >&2
  exit 2
fi

QMGR="$1"
shift
if [ "$1" = "-g" ]; then
  [ "$#" -eq 2 ] || { echo "usage: $0 <QMGR> <USER | -g GROUP>" >&2; exit 2; }
  PRINCIPAL="-g $2"
else
  [ "$#" -eq 1 ] || { echo "usage: $0 <QMGR> <USER | -g GROUP>" >&2; exit 2; }
  PRINCIPAL="-p $1"
fi

run() {
  echo "+ setmqaut -m $QMGR $*"
  # PRINCIPAL is split on purpose ("-p user" or "-g group").
  # shellcheck disable=SC2086
  setmqaut -m "$QMGR" "$@"
}

# Connect to the queue manager and inquire its attributes.
run -t qmgr $PRINCIPAL +connect +inq +dsp
# Inquire queues and read their attributes. No get, browse or put on application queues.
run -n '**' -t queue $PRINCIPAL +inq +dsp
# PCF requests go to the command queue.
run -n SYSTEM.ADMIN.COMMAND.QUEUE -t queue $PRINCIPAL +put +inq +dsp
# The tool creates a temporary reply queue from this model queue.
# A custom reply model queue (the "Reply Queue Model" connection field) needs the same grants as this one.
run -n SYSTEM.DEFAULT.MODEL.QUEUE -t queue $PRINCIPAL +dsp +get +put +inq
# Inquire channel status.
run -n '**' -t channel $PRINCIPAL +inq +dsp
