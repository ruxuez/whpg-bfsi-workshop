#!/bin/bash
## ======================================================================
## Container initialization script (runs as a systemd oneshot service)
## ======================================================================
## Root-level prep (chown, symlinks) is handled by init_root.sh via
## ExecStartPre — this script runs entirely as gpadmin, no sudo needed.
set -e

# Source environment variables and set MASTER_DATA_DIRECTORY
source /usr/local/greenplum-db/greenplum_path.sh
export MASTER_DATA_DIRECTORY=/data/master/gpseg-1

# Segments only need sshd running — the coordinator handles all init via SSH.
# Exit early on segment hosts to avoid running gpinitsystem concurrently
# with the coordinator (which would cause race conditions on data dirs).
if [ "$HOSTNAME" != "cdw" ]; then
  echo "Segment host $HOSTNAME ready (sshd running, waiting for coordinator)"
  exit 0
fi

# --- Everything below runs only on the coordinator (cdw) ---

# Wait for segment hosts to be reachable (sshd must be running).
# In DinD, containers start slower due to nested Docker overhead.
# Without this wait, ssh-copy-id hangs indefinitely if sshd isn't up yet.
echo "DNS resolver: $(cat /etc/resolv.conf | grep nameserver || echo 'none')"
for host in sdw1 sdw2; do
  echo "Waiting for $host sshd..."
  for i in $(seq 1 60); do
    ssh -o ConnectTimeout=2 -o StrictHostKeyChecking=no "$host" true 2>/dev/null && break
    sleep 2
  done
  if ! ssh -o ConnectTimeout=2 -o StrictHostKeyChecking=no "$host" true; then
    echo "ERROR: $host not reachable after 120s"
    echo "=== Diagnostics ==="
    echo "resolv.conf:"; cat /etc/resolv.conf
    echo "getent hosts $host:"; getent hosts "$host" 2>&1 || echo "getent failed"
    echo "ping $host:"; ping -c1 -W2 "$host" 2>&1 || echo "ping failed"
    echo "ssh verbose:"; ssh -v -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$host" true 2>&1 || true
    exit 1
  fi
  echo "  ✓ $host reachable"
done

# Populate known_hosts now that segments are reachable
ssh-keyscan -t rsa cdw sdw1 sdw2 > /home/gpadmin/.ssh/known_hosts 2>/dev/null

# Initialize multi node WarehousePG cluster
gpinitsystem -a \
             -c /tmp/gpinitsystem_config \
             -h /tmp/hostfile_gpinitsystem \
             --max_connections=100

printf "sdw1\nsdw2\n" >> /tmp/gpdb-hosts

# Allow any host to access the WarehousePG Cluster — coordinator AND all segments.
# Without this on segments, gprecoverseg fails with "no pg_hba.conf entry" errors
# because it connects to segment Postgres instances from the coordinator's IP.
echo 'host all all 0.0.0.0/0 trust' >> /data/master/gpseg-1/pg_hba.conf
for dir in /data1/primary/gpseg*/pg_hba.conf /data2/primary/gpseg*/pg_hba.conf\
           /data1/mirror/gpseg*/pg_hba.conf /data2/mirror/gpseg*/pg_hba.conf; do
  [ -f "$dir" ] && echo 'host all all 0.0.0.0/0 trust' >> "$dir"
done
# Also apply to segment hosts via SSH
for host in sdw1 sdw2; do
  ssh -o StrictHostKeyChecking=no "$host" bash -c '"
    for f in /data1/primary/gpseg*/pg_hba.conf /data2/primary/gpseg*/pg_hba.conf\
             /data1/mirror/gpseg*/pg_hba.conf /data2/mirror/gpseg*/pg_hba.conf; do
      [ -f \"\$f\" ] && echo \"host all all 0.0.0.0/0 trust\" >> \"\$f\"
    done
  "' || echo "WARNING: Could not update pg_hba on $host"
done
gpstop -u

psql -d template1 -c "ALTER USER gpadmin PASSWORD 'changeme@123'"

cat <<-'EOF'

======================================================================
Sandbox: WarehousePG Database Cluster details
======================================================================

EOF

echo "Current time: $(date)"
source /etc/os-release
echo "OS Version: ${NAME} ${VERSION}"

# Display version and cluster configuration
psql -P pager=off -d template1 -c "SELECT VERSION()"
psql -P pager=off -d template1 -c "SELECT * FROM gp_segment_configuration ORDER BY dbid"
psql -P pager=off -d template1 -c "SHOW optimizer"

# Configure PGAA: two-phase restart required.
# Phase 1: Add PGAA to shared_preload_libraries so hooks register.
gpconfig -c shared_preload_libraries -v pgaa
gpstop -a -M fast -r

# Phase 2: Now that PGAA hooks are loaded, set PGAA GUCs.
# These GUCs are sighup-reloadable — use gpstop -u (reload) instead of
# full restart to save ~25s on cold boot.
gpconfig -c pgaa.enable_maintenance_worker -v true
gpconfig -c pgaa.maintenance_worker_sleep_interval -v 1s
psql demo -c 'CREATE EXTENSION IF NOT EXISTS PGAA CASCADE'
gpstop -u

# Verify PGAA setup
echo ""
echo "PGAA Version:"
psql -P pager=off -d demo -c 'SELECT pgaa.pgaa_version()'

echo ""
echo "Installing PGVECTOR..."

# Step 1: Create extension VECTOR
psql -d demo -c 'CREATE EXTENSION IF NOT EXISTS vector CASCADE'

echo ""
echo "Installed Extensions:"
psql -P pager=off -d demo -c '\dx'

echo ""
echo "Configuring MADLIB..."

# Step 1: Install 
/usr/local/madlib/bin/madpack -s madlib -p greenplum -c gpadmin@localhost:5432/demo install

# Step 2: Check install
# /usr/local/madlib/bin/madpack -s madlib -p greenplum -c gpadmin@localhost:5432/demo install-check

echo ""
echo "Schemas:"
psql -P pager=off -d demo -c '\dn'

# echo ""
# echo "Installing Python dependencies..."

# # Ensure pip is up to date
# sudo python3 -m pip install --upgrade pip

# sudo python3 -m pip install flask


touch /data/master/gpinitsystem_complete

echo "
===========================
=  DEPLOYMENT SUCCESSFUL  =
===========================


======================================================================
 __          __            _                          _____   _____
 \ \        / /           | |                        |  __ \ / ____|
  \ \  /\  / /_ _ _ __ ___| |__   ___  _   _ ___  ___| |__) | |  __
   \ \/  \/ / _\` | '__/ _ \ '_ \ / _ \| | | / __|/ _ \  ___/| | |_ |
    \  /\  / (_| | | |  __/ | | | (_) | |_| \__ \  __/ |    | |__| |
     \/  \/ \__,_|_|  \___|_| |_|\___/ \__,_|___/\___|_|     \_____|

======================================================================"
