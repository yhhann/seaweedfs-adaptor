#!/bin/sh

checkdir(){
  directory=$1
  if [ ! -d $directory ];then
    mkdir -p $directory
  fi
  return 0
}


cd `dirname $0`
SEEDS_ADDR="127.0.0.1:9333,127.0.0.1:9334,127.0.0.1:9335"
REPLICATION="010"
DATA_CENTER=""
RACK=""
DOMAIN=1
CONCURRENT_NUM=16
FILE_COUNT=1000
FILE_SIZE=1024
FID_FILE="/tmp/benchmark_list.txt"
CHUNK_SIZE=524288
ENABLE_READ=true
ENABLE_WRITE=true
ENABLE_REMOVE=true
LOG_DIR="`pwd`/logs"
LOG_LEVEL=2

checkdir $LOG_DIR
bin/benchmark -seeds="$SEEDS_ADDR" -replication="$REPLICATION" \
    -dataCenter="$DATA_CENTER" -rack="$RACK" \
    -domain="$DOMAIN" -c="$CONCURRENT_NUM" \
    -n="$FILE_COUNT" -size="$FILE_SIZE" \
    -id-file="$FID_FILE" -weed-chunk-size="$CHUNK_SIZE" \
    -read="$ENABLE_READ" -write="$ENABLE_WRITE" -remove="$ENABLE_REMOVE" \
    -log_dir="$LOG_DIR" -v="$LOG_LEVEL" -logtostderr=false &
