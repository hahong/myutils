#!/bin/bash
squeue -o "%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R   %C" $@
