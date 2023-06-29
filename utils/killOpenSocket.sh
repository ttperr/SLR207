#!/bin/bash

login="tperrot-21"

# bash to execute on every line of ../data/machines.txt : ssh login@line fuser -k 8888/tcp

# shellcheck disable=SC2095
while read -r line; do
  ssh "$login"@"$line" fuser -k 8888/tcp
done < ../data/machines.txt