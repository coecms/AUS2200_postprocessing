#!/usr/bin/env bash
#PBS -l ncpus=104
#PBS -l mem=470GB
#PBS -l jobfs=100GB
#PBS -l walltime=48:00:00
#PBS -l storage=gdata/hh5+scratch/ly62
#PBS -q normalsr
#PBS -P ui47

### Construct a files list
###for i in $( find $PWD/experiments/flood22-restart/ -type f -name \*.nc ); do python3 -c 'import sys; pl=sys.argv[1].split("/"); fn=pl[-1].split(".")[0]; print(f"/scratch/ly62/dr4292/cylc-run/{pl[7]}/share/cycle/{pl[8]}/aus2200/d0198/RA3/um/{fn}+{sys.argv[1]}")' $i; done > restart_files.txt

export LAUNCHER="singularity -s exec --bind /etc,/half-root,/local,/ram,/run,/system,/usr,/var/lib/sss,/var/run/munge --overlay=$PBS_JOBFS/analysis3-23.01.sqsh /g/data/hh5/public/apps/cms_conda/etc/base.sif"

module use /g/data/hh5/public/modules
module load conda_concept/analysis3-unstable

cp "${CONTAINER_OVERLAY_PATH}" "${PBS_JOBFS}"
export CONTAINER_OVERLAY_PATH="${PBS_JOBFS}"/"${CONTAINER_OVERLAY_PATH##*/}"

#export SCHED_FILE=$( mktemp -u -p $PWD )

#trap "rm -f ${SCHED_FILE}" EXIT

#$LAUNCHER dask scheduler --scheduler-file "${SCHED_FILE}" &

#while ! [[ -f "${SCHED_FILE}" ]]; do sleep 10; done

#$LAUNCHER mpirun -np 24 --map-by numa --bind-to core dask worker --nworkers 1 --nthreads 1 --scheduler-file "${SCHED_FILE}" &

cd $PBS_O_WORKDIR

$LAUNCHER xargs -a "${FILELIST}" -P 12 -n 1 ./nc_verify.py
