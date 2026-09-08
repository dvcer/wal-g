#!/bin/sh
set -e -x

# shellcheck disable=SC1091
. /usr/local/export_common.sh

export WALE_S3_PREFIX=s3://mysql-pitr-binlogserver-until-lmt-bucket
export WALG_MYSQL_BINLOG_SERVER_HOST="localhost"
export WALG_MYSQL_BINLOG_SERVER_PORT=9306
export WALG_MYSQL_BINLOG_SERVER_USER="walg"
export WALG_MYSQL_BINLOG_SERVER_PASSWORD="walgpwd"
export WALG_MYSQL_BINLOG_SERVER_ID=99
export WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE="sbtest@tcp(127.0.0.1:3306)/sbtest"

mysqld --initialize --init-file=/etc/mysql/init.sql
service mysql start

# These rows will be included in the backup.
mysql -e "CREATE TABLE sbtest.pitr(id VARCHAR(32), ts DATETIME)"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_01', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_02', NOW())"
mysql -e "FLUSH BINARY LOGS"
BACKUP_BINLOG=$(mysql -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
wal-g backup-push

# Taking a backup may rotate the binlog; capture the actual post-backup file.
INCLUDED_BINLOG=$(mysql -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_03', NOW())"
mysql -e "INSERT INTO sbtest.pitr VALUES('from_binlog_04', NOW())"
mysql -e "FLUSH BINARY LOGS"
wal-g binlog-push

# This file will be archived after the last-modified cutoff.
EXCLUDED_BINLOG=$(mysql -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
mysql -e "INSERT INTO sbtest.pitr VALUES('lmt_ignored_01', NOW())"
sleep 1
DT1=$(date3339)
sleep 1
mysql -e "INSERT INTO sbtest.pitr VALUES('after_pitr_01', NOW())"
mysql -e "FLUSH LOGS"
wal-g binlog-push

mysql_kill_and_clean_data
wal-g backup-fetch LATEST
chown -R mysql:mysql "$MYSQLDATA"
service mysql start || (cat /var/log/mysql/error.log && false)
mysql_set_gtid_purged

BINLOG_SERVER_LOG=/tmp/binlog_server_until_lmt.log

# DT1 is used as both PITR time (--until) and the binlog last-modified cutoff
# (--until-binlog-last-modified-time). BACKUP_BINLOG and INCLUDED_BINLOG were
# pushed before DT1. EXCLUDED_BINLOG was pushed after DT1, so it must be
# filtered out even though lmt_ignored_01 is valid data before PITR time.
WALG_LOG_LEVEL="DEVEL" wal-g binlog-server \
    --since LATEST \
    --until "$DT1" \
    --until-binlog-last-modified-time "$DT1" \
    2>&1 | tee "$BINLOG_SERVER_LOG" &
walg_pid=$!

sleep 3
mysql -e "STOP SLAVE"
mysql -e "SET GLOBAL SERVER_ID = 123"
mysql -e "CHANGE MASTER TO MASTER_HOST=\"127.0.0.1\", MASTER_PORT=9306, MASTER_USER=\"walg\", MASTER_PASSWORD=\"walgpwd\", MASTER_AUTO_POSITION=1"
mysql -e "START SLAVE"

wait "$walg_pid"

mysqldump sbtest > /tmp/dump_after_pitr_until_lmt

# Rows from the backup and binlogs archived before the last-modified cutoff.
grep -w 'from_binlog_01' /tmp/dump_after_pitr_until_lmt
grep -w 'from_binlog_02' /tmp/dump_after_pitr_until_lmt
grep -w 'from_binlog_03' /tmp/dump_after_pitr_until_lmt
grep -w 'from_binlog_04' /tmp/dump_after_pitr_until_lmt

# lmt_ignored_01 is in EXCLUDED_BINLOG, which was pushed to S3 after DT1 (LMT),
# so it must be absent even though the data is before PITR time
if grep -w 'lmt_ignored_01' /tmp/dump_after_pitr_until_lmt; then
    echo "ERROR: found row from a binlog beyond the last-modified cutoff"
    exit 1
fi

# rows after pitr time must be absent
if grep -w 'after_pitr_01' /tmp/dump_after_pitr_until_lmt; then
    echo "ERROR: found row written after the PITR cutoff"
    exit 1
fi

# The source files archived before the last-modified cutoff must be streamed.
grep -F "Streaming $WALG_MYSQL_BINLOG_DST/$BACKUP_BINLOG to replica" "$BINLOG_SERVER_LOG"
grep -F "Streaming $WALG_MYSQL_BINLOG_DST/$INCLUDED_BINLOG to replica" "$BINLOG_SERVER_LOG"

# The source file archived after the last-modified cutoff must not be streamed.
if grep -F "Streaming $WALG_MYSQL_BINLOG_DST/$EXCLUDED_BINLOG to replica" "$BINLOG_SERVER_LOG"; then
    echo "ERROR: streamed $EXCLUDED_BINLOG beyond the last-modified cutoff"
    exit 1
fi
