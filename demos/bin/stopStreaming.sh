#!/usr/bin/env bash
get_abs_script_path() {
  pushd . >/dev/null
  cd "$(dirname "$0")"
  appdir=$(pwd)
  popd  >/dev/null
}

get_abs_script_path

force_kill=true
application_id=$(yarn application -list | grep castar_streaming | grep -oe "application_[0-9]*_[0-9]*")

if [ ! -z $application_id ]; then
    yarn application -kill ${application_id}
fi

pids=$(ps aux | grep streaming | grep -v grep | awk '{print $2}')

if [ ! -z "$pids" ]; then
    kill -9 $pids
fi