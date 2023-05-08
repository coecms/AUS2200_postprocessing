#!/usr/bin/env python

import mule

prefix='/g/data/hh5/tmp/WACI-Hackathon-2023/admin/BARRA2'
dates=( '20220219T1200Z', '20220219T1800Z', '20220220T0000Z', '20220220T0600Z', '20220220T1200Z', '20220220T1800Z', '20220221T0000Z', '20220221T0600Z' ) 
frame_fn='frame_barra-a'

in_mfs=[ mule.load_umfile(f"{prefix}/{d}/{frame_fn}") for d in dates ]
out_mf = in_mfs[0].copy()

for i,in_mf in enumerate(in_mfs):
    for f in in_mf.fields:
        if f.lbuser4 == 33:
            this_day=f.lbdat
            start_hr=f.lbhr
            if i==0:
                out_mf.fields.append(f)
            break
    for f in in_mf.fields:
        if f.lbuser4 == 33: continue
        if f.lbdat != this_day: continue
        if f.lbhr > start_hr + 5: continue
        out_mf.fields.append(f)

out_mf.to_file('test_cb000')