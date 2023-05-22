#!/bin/bash

force=0
stop=0
function bail()
{
  if [[ $force == 1 ]]; then
    echo "Force exit."
    exit
  else
    echo "Restart disabled."
    stop=1
    force=1
  fi
}

trap bail SIGINT

while [[ $stop == 0 ]]
do
  eval "$@";
  if [[ "$?" != 0 ]]; then
    osascript -e "display notification \"Process crashed, restarting\" with title \"Reflect Dev\" subtitle \"$@\"";
  fi
done
