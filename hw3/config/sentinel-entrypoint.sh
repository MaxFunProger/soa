#!/bin/sh
set -e
for i in $(seq 1 90); do
  IP=$(getent hosts redis-master 2>/dev/null | awk '{print $1; exit}')
  if [ -n "$IP" ]; then break; fi
  sleep 1
done
if [ -z "$IP" ]; then
  echo "cannot resolve redis-master"
  exit 1
fi
sed "s/%%MASTER_HOST%%/$IP/g" /etc/redis/sentinel.conf.template > /tmp/sentinel.conf
exec redis-sentinel /tmp/sentinel.conf
