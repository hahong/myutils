#!/bin/bash

test "$MAXDEPTH" || MAXDEPTH=2

find . -maxdepth $MAXDEPTH -name "$1" | \
	while read f; do
		#grep -iHn --color get_best {}  ';'
		#grep -i --color $2 $3 $4 $5 $6 "$f" && echo "-->" "$f" && echo
		grep -Hl --color $2 $3 $4 $5 $6 "$f" && grep -n --color $2 $3 $4 $5 $6 "$f" && echo
	done
