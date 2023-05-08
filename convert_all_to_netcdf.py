#!/usr/bin/env python3

import multiprocessing as mp
import um2netcdf4
import collections
import os
import argparse

from dask.distributed import Client
from glob import glob

#import dask.config

parser = argparse.ArgumentParser(description="Convert UM files to netcdf")
parser.add_argument("-p","--prefix")
parser.add_argument("-s","--sched-file")
parser.add_argument("-o","--out-prefix")
ns = parser.parse_args()

def run_um2nc(inf,outf,a):
    #dask.config.set({'array.chunk-size':'128 MiB'})
    #client = Client(scheduler_file=ns.sched_file)
    os.makedirs(os.path.dirname(outf),exist_ok=True)
    um2netcdf4.process(inf,outf,a)
    if os.stat(outf).st_size < 1048576:
        os.remove(outf)

ds=ns.prefix.split('/')[-1]
#prefix="/scratch/ly62/dr4292/cylc-run/u-cs142-waci-longrun/share/cycle/"

#sched_file=sys.argv[2]

Args = collections.namedtuple('Args','nckind compression simple nomask hcrit verbose include_list exclude_list nohist use64bit')
args = Args(3, 4, True, False, 0.5, False, None, None, False, False)

um2nc_par_args=[]
um2nc_ser_args=[]

#for day in glob('/scratch/ly62/dr4292/cylc-run/u-cs142-waci-longrun/share/cycle/*'):

flist=glob(ns.prefix+'/aus2200/d0198/RA3/um/umnsa_*')
flist.extend(glob(ns.prefix+'/aus2200/d0198/RA3/um/umnsaa_*'))

for l in flist:
    if "_mdl_" in l:
        #um2nc_ser_args.append((l,f"/scratch/ly62/dr4292/experiments/flood22-continuous/netcdf/{ds}/{'/'.join(l.split('/')[-5:])}.nc",args))
        um2nc_ser_args.append((l,f"{ns.out_prefix}/{ds}/{'/'.join(l.split('/')[-5:])}.nc",args))
    else:
        um2nc_par_args.append((l,f"{ns.out_prefix}/{ds}/{'/'.join(l.split('/')[-5:])}.nc",args))

print(f"{ds}: {len(um2nc_par_args)} files to process in parallel")
print(f"{ds}: {len(um2nc_ser_args)} files to process in serial")

with mp.Pool(min(6,len(um2nc_par_args))) as pool:
    pool.starmap(run_um2nc,um2nc_par_args)

#for a in um2nc_ser_args:
#    run_um2nc(*a)
with mp.Pool(min(2,len(um2nc_ser_args))) as pool:
    pool.starmap(run_um2nc,um2nc_ser_args)