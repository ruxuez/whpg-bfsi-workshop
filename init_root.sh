#!/bin/bash
## ======================================================================
## Root-level preparation (runs as ExecStartPre before init_system.sh)
## ======================================================================
## These operations need root and must run before the gpadmin init script.
## Separated from init_system.sh to avoid sudo inside the container
## (sudo + PAM can fail in CI/containerized environments).
set -e

ln -sf /usr/bin/python2.7 /usr/bin/python 2>/dev/null || true
rm -rf /run/nologin

chown -R gpadmin:gpadmin /usr/local/greenplum-db \
                          /tmp/gpinitsystem_config \
                          /tmp/hostfile_gpinitsystem \
                          /tmp/gpdb-hosts

if [ "$(hostname)" = "cdw" ]; then
  chown -R gpadmin:gpadmin /data
else
  mkdir -p /data1/mirror /data2/mirror
  chown -R gpadmin:gpadmin /data1
  chown -R gpadmin:gpadmin /data2
fi
