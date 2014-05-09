#!/bin/bash

#WP=`tmux display-message -p "#I.#P"`

if [ "$1" == "-c" ]; then
	tmux set-buffer "$2" 
else
	tmux select-pane -t :.+
	if [ "$1" == "-n" ]; then
		tmux send-keys "$2" 
	else
		tmux send-keys "$1" C-m
	fi
	tmux select-pane -t :.-
fi	

bash -c 'sleep 0.2; tmux send-keys " "' & 
