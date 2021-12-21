#!/bin/bash
cd /home
chmod a+x *
./tongsumex -conf cfg/4G_cell.cfg -log "$1" -starttime "$2"
