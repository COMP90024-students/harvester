#!/bin/bash
# 1 nodes, 8 tasks per node = 8 cores
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --time=0-12:00:00


# Load required modules
source /usr/local/module/spartan_new.sh
module load gcc/8.3.0
module load python/3.7.4
module load openmpi/3.1.4

echo "Searching for topics"
time mpiexec -n 8 python processParallelTweets.py
