#!/usr/bin/env bash
#PBS -l walltime=6:00:00
#PBS -l ncpus=48
#PBS -l jobfs=400GB
#PBS -l mem=192GB
#PBS -l storage=gdata/hh5+scratch/v45+gdata/v45

cd $PBS_O_WORKDIR

module use /g/data/hh5/admin/conda_concept/modules
module load conda_concept/analysis3-unstable

dask scheduler --scheduler-file sched_"${FILE_PREFIX}".json &

while ! [[ -f sched_"${FILE_PREFIX}".json ]]; do sleep 10; done

mpirun -np 24 --map-by socket --bind-to core dask worker --nworkers 1 --nthreads 1 --scheduler-file sched_"${FILE_PREFIX}".json &

sleep 20

python3 full_field_from_pressure_levels_2.py -s "${STASH_CODE}" -p "${FILE_PREFIX}" ${DO_RHO} ${REGRID} -f "${FREQ:-1}"

### python3 full_field_from_pressure_levels_2.py -s m01s00i002 -p u_component_of_wind -r -g
### python3 full_field_from_pressure_levels_2.py -s m01s00i003 -p v_component_of_wind -r -g
### python3 full_field_from_pressure_levels_2.py -s m01s00i150 -p w_component_of_wind
### python3 full_field_from_pressure_levels_2.py -s m01s15i101 -p geopotential_height -f 6