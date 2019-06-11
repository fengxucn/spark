#!/usr/bin/env bash
get_abs_script_path() {
  pushd . >/dev/null
  cd "$(dirname "$0")"
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

while true; do
    application_id=$(yarn application -list | grep castar_streaming | grep -oe "application_[0-9]*_[0-9]*")
    if [ -z "$application_id" ]; then
        echo "$(date) : streaming not found. trying to start it..."
        nohup $appdir/streaming.sh $1 >> $appdir/../../logs/out.log &
    else
        application_status=$(yarn application -status ${application_id} | grep "State : RUNNING")
        if [ -z "$application_status" ]; then
            echo "$(date) : streaming ($application_id) not running, $application_status. trying to start it..."
            yarn application -kill ${application_id}
            nohup $appdir/streaming.sh $1 >> $appdir/../../logs/out.log &
        else
            echo "$(date) : streaming is $application_status, do nothing."
        fi
    fi
    sleep 10m
done

