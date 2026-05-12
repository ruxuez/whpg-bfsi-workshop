FROM rockylinux/rockylinux:8

ENV container=docker

RUN dnf update -y && \
  dnf install -y epel-release && \
  dnf clean all

RUN echo root:changeme@123 | chpasswd && \
  dnf makecache && \
  dnf install -y epel-release && \
  dnf install -y --enablerepo=epel \
  passwd \
  sudo \
  systemd \
  systemd-libs \
  apr apr-util \
  bash \
  bzip2 \
  curl \
  iproute \
  krb5-libs \
  less \
  libxml2 \
  libyaml \
  openldap \
  openssh \
  openssh-clients \
  openssh-server \
  perl \
  readline \
  rsync \
  sed \
  tar \
  which \
  zip \
  zlib \
  libuuid \
  libevent \
  libzstd \
  libuv \
  iputils \
  net-tools \
  python3 \
  libicu \
  xerces-c \
  python3-psycopg2 \
  python3-psutil \
  python3-pyyaml \
  clang \
  llvm \
  llvm-libs \
  m4

  # Clean up systemd units that are unnecessary in a container
RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [ $i == systemd-tmpfiles-setup.service ] || rm -f $i; done); \
rm -f /lib/systemd/system/multi-user.target.wants/*; \
rm -f /etc/systemd/system/*.wants/*; \
rm -f /lib/systemd/system/local-fs.target.wants/*; \
rm -f /lib/systemd/system/sockets.target.wants/*udev*; \
rm -f /lib/systemd/system/sockets.target.wants/*initctl*; \
rm -f /lib/systemd/system/basic.target.wants/*; \
rm -f /lib/systemd/system/anaconda.target.wants/*

RUN --mount=type=secret,id=repo_token bash -c 'ls /run/secrets && curl -1Lf https://downloads.enterprisedb.com/$(cat /run/secrets/repo_token)/gpsupp/setup.rpm.sh | sudo bash'
RUN dnf -y install \
  warehouse-pg-7 \
  edb-whpg7-pgvector \
  edb-whpg7-pgfs \
  edb-whpg7-madlib \
  edb-whpg7-pgaa


# (EDB repo cleanup happens after PGAA install below — both repo configs removed together)

COPY gpinitsystem_config /tmp/
COPY hostfile_gpinitsystem /tmp/
COPY init_system.sh /tmp/
COPY init_root.sh /tmp/
COPY start_cluster.sh /tmp/


RUN echo "cdw" > /tmp/gpdb-hosts && \
  echo "/usr/local/lib" >> /etc/ld.so.conf && \
  echo "/usr/local/lib64" >> /etc/ld.so.conf && \
  ldconfig && \
  chmod 777 /tmp/gpinitsystem_config && \
  chmod 777 /tmp/hostfile_gpinitsystem && \
  chmod 777 /tmp/init_system.sh && \        
  chmod 755 /tmp/init_root.sh && \
  chmod 755 /tmp/start_cluster.sh && \
  hostname > ~/orig_hostname && \
  /usr/sbin/groupadd gpadmin && \
  /usr/sbin/useradd  gpadmin -g gpadmin -G wheel && \
  setcap cap_net_raw+ep /usr/bin/ping && \
  echo "changeme@123"| passwd --stdin gpadmin && \
  echo "gpadmin        ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
  echo "root           ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers && \
  echo "export MASTER_DATA_DIRECTORY=/data/master/gpseg-1" >> /home/gpadmin/.bashrc && \
  echo "export COORDINATOR_DATA_DIRECTORY=/data/master/gpseg-1" >> /home/gpadmin/.bashrc && \
  echo "source /usr/local/greenplum-db/greenplum_path.sh" >> /home/gpadmin/.bashrc && \
  sed -i 's/^session    required     pam_limits.so/#session    required     pam_limits.so/' /etc/pam.d/sudo && \
  ssh-keygen -A && \
  # sshd_config uses first-match — must sed existing lines, not append
  sed -i 's/^#\?UsePAM.*/UsePAM no/' /etc/ssh/sshd_config && \
  sed -i 's/^#\?GSSAPIAuthentication.*/GSSAPIAuthentication no/' /etc/ssh/sshd_config && \
  # Generate deterministic SSH keys for gpadmin at build time so all
  # containers from this image can SSH to each other without ssh-copy-id.
  # This survives container recreation (unlike runtime-generated keys).
  mkdir -p /home/gpadmin/.ssh && \
  ssh-keygen -t rsa -b 4096 -C gpadmin -f /home/gpadmin/.ssh/id_rsa -P "" -q && \
  cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys && \
  chmod 700 /home/gpadmin/.ssh && \
  chmod 600 /home/gpadmin/.ssh/authorized_keys && \
  chown -R gpadmin:gpadmin /home/gpadmin/.ssh

# Install the WHPG init script as a systemd oneshot service so it runs
# once after sshd is available (needed for gpinitsystem).
RUN printf '[Unit]\n\
Description=Initialize WarehousePG cluster\n\
After=sshd.service network.target\n\
Requires=sshd.service\n\
ConditionPathExists=!/data/master/gpinitsystem_complete\n\
\n\
[Service]\n\
Type=oneshot\n\
ExecStartPre=+/tmp/init_root.sh\n\
User=gpadmin\n\
ExecStart=/tmp/init_system.sh\n\
RemainAfterExit=yes\n\
StandardOutput=journal+console\n\
StandardError=journal+console\n\
\n\
[Install]\n\
WantedBy=multi-user.target\n' > /etc/systemd/system/whpg-init.service && \
    printf '[Unit]\n\
Description=Start WarehousePG cluster (already initialized)\n\
After=sshd.service network.target\n\
Requires=sshd.service\n\
ConditionPathExists=/data/master/gpinitsystem_complete\n\
\n\
[Service]\n\
Type=oneshot\n\
User=gpadmin\n\
Environment=MASTER_DATA_DIRECTORY=/data/master/gpseg-1\n\
ExecStart=/tmp/start_cluster.sh\n\
RemainAfterExit=yes\n\
StandardOutput=journal+console\n\
StandardError=journal+console\n\
TimeoutStartSec=300\n\
\n\
[Install]\n\
WantedBy=multi-user.target\n' > /etc/systemd/system/whpg-start.service && \
    systemctl enable sshd.service && \
    systemctl enable whpg-init.service && \
    systemctl enable whpg-start.service


# Remove EDB repo configs so no token or repo URL leaks into image layers
RUN rm -f /etc/yum.repos.d/enterprisedb-*.repo \
/etc/yum.repos.d/downloads_enterprisedb_com_*.repo && \
dnf clean all

# 1. Install Python 3.9 and development headers
RUN dnf install -y python39 python39-devel python39-pip && \
    dnf clean all

# 2. Install Lab 1 & Lab 2 dependencies system-wide
# We use the absolute path to the 3.9 binary to be 100% sure
RUN /usr/bin/python3.9 -m pip install --upgrade pip && \
    /usr/bin/python3.9 -m pip install \
    flask \
    psycopg2-binary \
    "pyiceberg[sql-sqlite,pyiceberg-core]" \
    pandas plotly dash dash-bootstrap-components && \
    /usr/bin/python3.9 -m pip install pyarrow --only-binary=:all:

# USER gpadmin
# ENV USER=gpadmin
# WORKDIR /home/gpadmin

EXPOSE 5432 22

STOPSIGNAL SIGRTMIN+3

# systemd as PID 1
CMD ["/usr/sbin/init"]
# CMD ["bash","-c","/tmp/init_system.sh"]