#!/bin/bash

login="tperrot-21"

[[ -e index.html ]] && rm index.html

wget tp.telecom-paris.fr
# shellcheck disable=SC2062
# shellcheck disable=SC2002
ok_machines=$(cat index.html | grep "</tr>" | sed 's/<\/tr>/\n/g' | grep OK | grep -o tp-[1-9][a-z][0-9]*-[0-9]*  | awk '{print $1".enst.fr"}')

tmp=$(echo "$ok_machines" | tr "\n" " ")

machines=()

read -ra machines <<< "$tmp"

# shellcheck disable=SC2207
shuffled_machines=($(shuf -e "${machines[@]}"))

working_count=1

# shellcheck disable=SC2188
> "../data/machines.txt"

for machine in "${shuffled_machines[@]}"; do
  ssh -q -o StrictHostKeyChecking=no -o BatchMode=yes -o ConnectTimeout=3 $login@"$machine" "exit"
  # shellcheck disable=SC2181
  [[ $? -eq 0 ]] && echo "$machine" >> "../data/machines.txt" && ((working_count++))
  [[ $working_count -eq $1 ]] && break
done

rm index.html