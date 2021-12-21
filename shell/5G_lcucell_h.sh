#!/bin/bash
cd /home
chmod a+x *
./tongsumex -conf cfg/5G_lducell.cfg -log "$1" -starttime "$2"
