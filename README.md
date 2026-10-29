# Analytics Database (WarehousePG + PGAA + PgVector + MADlib)

Multi-node WarehousePG cluster with different Analytics and AI extentions.

## Architecture

This setup creates a distributed WarehousePG cluster with:

- **Coordinator** (`cdw`): Master node managing query planning and coordination
- **Segment 1** (`sdw1`): Data processing node with 2 primary
- **Segment 2** (`sdw2`): Data processing node with 2 primary

Total: 4 primary segments. No mirrors for high availability.

## Prerequisites

- EDB Subscription Token (set as environment variable)
- Catalog component must be running (RustFS)
- Network: `converged-analytics-network` (created by catalog stack)

## Quick Start

### 1. Build and Start Cluster

```bash
cd analytics-db
echo "EDB_SUBSCRIPTION_TOKEN=\"${EDB_SUBSCRIPTION_TOKEN}\"" > .env
docker-compose build
docker-compose up -d 
```

### 2. Monitor Startup

Watch the coordinator logs (~5-10 minutes for first-time init):

```bash
docker logs -f cdw
```

Wait for the "DEPLOYMENT SUCCESSFUL" banner.

### 3. Verify Cluster

```bash
# Check all containers are running
docker ps | grep -E "cdw|sdw"

# Check cluster status
docker exec -u gpadmin cdw \
  bash -c " \
    source /usr/local/greenplum-db/greenplum_path.sh && \
    export COORDINATOR_DATA_DIRECTORY=/data/master/gpseg-1 && \
    gpstate \
"
```

## Configuration Files

- **docker-compose.yml**: Multi-container setup (coordinator + 2 segments)
- **Dockerfile**: Rocky Linux 8 based image with WarehousePG 7, PGAA, PGFS
- **gpinitsystem_config**: Cluster initialization settings
- **hostfile_gpinitsystem**: List of segment hosts (sdw1, sdw2)
- **init_system.sh**: Container startup script that:
  - Starts SSH daemon for inter-container communication
  - Sets up SSH keys for passwordless access
  - Runs `gpinitsystem` to create the distributed cluster
  - Configures pg_hba.conf for external access

## Credentials

- **User**: `gpadmin`
- **Password**: `changeme@123`
- **Database**: `demo` (created during initialization)

## Connecting

From host machine:

```bash
PGPASSWORD=changeme@123 psql -h localhost -p 5432 -U gpadmin -d demo
```


## Volumes

- **master**: Coordinator node data directory
- **sdw1_primary1, sdw1_primary2**: Segment 1 primary data directories
- **sdw2_primary1, sdw2_primary2**: Segment 2 primary data directories

## Assignments

Check Markdown files in `Assignment` directory for labs.

## Troubleshooting

### Check container logs

```bash
# Coordinator
docker logs cdw

# Segments
docker logs sdw1
docker logs sdw2
```

### Restart cluster

```bash
# From inside coordinator container
docker exec cdw bash -ic "source /usr/local/greenplum-db/greenplum_path.sh && gpstop -a -M fast && gpstart -a"
```

### Access coordinator shell

```bash
docker exec -it cdw bash -ic bash
```

### Cluster won't initialize

- Check that all three containers are running
- Verify SSH connectivity between containers:
  ```bash
  docker exec cdw ssh sdw1 hostname
  docker exec cdw ssh sdw2 hostname
  ```
- Check segment host ownership of data directories:
  ```bash
  docker exec sdw1 ls -la /data1 /data2
  ```

### Check PGAA configuration

```bash
PGPASSWORD=changeme@123 psql -h localhost -p 5432 -U gpadmin -d demo -c "SELECT * FROM pgaa.list_catalogs();"
PGPASSWORD=changeme@123 psql -h localhost -p 5432 -U gpadmin -d demo -c '\dx'
```

### Performance Tuning

```sql
-- Disable pushdown for debugging
SET pgaa.enable_groupby_pushdown = off;
SET pgaa.enable_join_pushdown = off;

-- Re-enable
SET pgaa.enable_groupby_pushdown = on;
SET pgaa.enable_join_pushdown = on;
```

## Stopping

```bash
docker-compose down
```

To remove volumes (destroys all data):

```bash
docker-compose down -v
```

## Rebuilding

If you need to completely rebuild the cluster:

```bash
# Stop and remove everything
docker-compose down -v

# Remove images
docker rmi whpg_cdw

# Rebuild and start
docker-compose build
docker-compose up -d
```
