#!/usr/bin/env bash
#PBS -l ncpus=26
#PBS -l mem=100GB
#PBS -l walltime=1:00:00
#PBS -l storage=gdata/hh5+gdata/access+scratch/ly62
#PBS -P ui47
#PBS -q normalsr

cd $PBS_O_WORKDIR

module use ~access/modules
module load pythonlib/um2netcdf4/2.1
module use /g/data/hh5/public/modules
module load conda_concept/analysis3-unstable

readarray -d'/' in_prefix_arr <<<"${IN_PREFIX}"

python3 ./convert_all_to_netcdf.py -s "${SCHED_FILE}" -p "${IN_PREFIX}" -o "${OUT_PREFIX}"/"${in_prefix_arr[5]%/}"