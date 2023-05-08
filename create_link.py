#!/usr/bin/env python3
from glob import glob
from pathlib import Path
import os

sequential=False
basedir="/g/data/hh5/tmp/WACI-Hackathon-2023/AUS2200/flood22-barra-soil/raw"
outdir=Path("/g/data/hh5/tmp/WACI-Hackathon-2023/AUS2200/flood22-barra-soil/history/atm/netCDF")

if sequential:
    file_list=[ Path(i) for i in glob(f'{basedir}/*/aus2200/d0198/RA3/um/umnsa_*') ]
    file_list.extend([ Path(i) for i in glob(f'{basedir}/*/aus2200/d0198/RA3/um/umnsaa_*') ])
else:
    file_list=[ Path(i) for i in glob(f'{basedir}/*/*/aus2200/d0198/RA3/um/umnsa_*') ]
    file_list.extend([ Path(i) for i in glob(f'{basedir}/*/*/aus2200/d0198/RA3/um/umnsaa_*') ])
for f in file_list:
    date='zzz'
    for i,token in enumerate(f.parts):
        if token.startswith('u-cs142'):
            date=token.split('-')[-1].split('T')[0]
            break
    try:
        if f.parts[i+1].startswith(date): continue
        file_date=f.parts[i+1][:-1]
    except IndexError:
        file_date=[ i for i in f.parts if i.startswith('20220') ][0][:-1]
    if f.name.startswith('umnsaa_'):
        out_fname=outdir / f'{f.stem}_{file_date}_6h{f.suffix}'
    else:
        out_fname=outdir / f'{f.stem}_1h{f.suffix}'
    os.makedirs(out_fname.parent,exist_ok=True)
    Path(out_fname).symlink_to(Path(f))
