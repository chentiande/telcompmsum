#!/bin/bash
cd /home
chmod a+x *
./tongsumex -conf cfg/tongsumex.cfg -log "$1" -starttime "$2"