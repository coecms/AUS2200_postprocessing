#!/usr/bin/env python3

import mule
import os

from glob import glob

prefix='/g/data/hh5/tmp/WACI-Hackathon-2023/admin/BARRA2'
#dates=( '20220219T1200Z', '20220219T1800Z', '20220220T0000Z', '20220220T0600Z', '20220220T1200Z', '20220220T1800Z', '20220221T0000Z', '20220221T0600Z' ) 
owd=os.getcwd()
os.chdir(prefix)
dates=glob('2022*Z')
os.chdir(owd)
frame_fn='frame_barra-a'

for d in dates:
    in_mf=mule.load_umfile(f"{prefix}/{d}/{frame_fn}")
    hr=int(d[9:11])
    for dh in range(7):
        out_fn=f"{prefix}/ra2_atmanl_cutout_{d}+{dh:03}"
        out_mf = in_mf.copy()
        for f in in_mf.fields:
            if f.lbhr == (hr+dh)%24:
                out_mf.fields.append(f)
            ### Orog is special, must advance validity time to match the rest of the fields
            elif f.lbuser4 == 33:
                outf=f.copy()
                outf.lbhr=(hr+dh)%24
                if (hr+dh)%24 == 0: outf.lbdat+=1
                ### Yuck
                if outf.lbdat==29:
                    outf.lbdat=1
                    outf.lbmon=3
                out_mf.fields.append(outf)
        out_mf.to_file(out_fn)
