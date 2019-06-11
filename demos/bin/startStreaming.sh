#!/usr/bin/env bash
get_abs_script_path() {
  pushd . >/dev/null
  cd "$(dirname "$0")"
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

echo "Try to stop the current running streaming first..."
$appdir/stopStreaming.sh

echo "Try to start new streaming now..."

nohup $appdir/streaming/startStreaming.sh $1 >> $appdir/../logs/streamingStatus.log &