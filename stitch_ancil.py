#!/usr/bin/env python3

import iris

prefix='/g/data/hh5/tmp/WACI-Hackathon-2023/admin/BARRA2'
dates=( '20220219T1200Z', '20220219T1800Z', '20220220T0000Z', '20220220T0600Z', '20220220T1200Z', '20220220T1800Z', '20220221T0000Z', '20220221T0600Z' ) 
frame_fn='frame_barra-a'

cll=[ iris.load(f'{prefix}/{d}/{frame_fn}') for d in dates ]

print("Cubes loaded")

for cl in cll:
    for i,c in enumerate(cl):
        c.data
        c.replace_coord(cll[0][i].coord('forecast_reference_time'))
        #c=c[:6]

print("Coords replaced")

out_cl=iris.cube.CubeList()
for i,_ in enumerate(cll[0]):
    to_concat=[ j[i][:6] for j in cll  if j[i].name() != "surface_altitude" ]
    if to_concat:
        out_cl.append( iris.cube.CubeList( to_concat ).concatenate_cube() )
    else:
        out_cl.append( cll[0][i] )

print("Created concatenated cubes")

iris.save(out_cl,"glm_cb_000",saver="pp")