#! /usr/bin/env zsh

while :
do
	echo "starting drop caches"
	echo 3 | sudo tee /proc/sys/vm/drop_caches >> tmp/log-drop-caches
	sleep 20s
done
