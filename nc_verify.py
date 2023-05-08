#!/usr/bin/env python3

import xarray as xr
import iris
import numpy as np
from multiprocessing import Pool
import sys
from dask.distributed import Client
import os

#nc_prefix="/scratch/ly62/dr4292/experiments/flood22-continuous/netcdf"
#um_prefix="/scratch/ly62/dr4292/cylc-run/u-cs142-waci-longrun/share/cycle"

#ts="20220221T1200Z"
#fn="umnsa_cldrad_20220221T1400"
#fn="umnsaa_pa000"
tol=1.e-6
eps=1.e-15

def coord_translate(coord):
    if coord.startswith("model_rho_level_number"): return "model_level_number"
    if coord.startswith("model_theta_level_number"): return "model_level_number"
    if coord.startswith("lat") : return "latitude"
    if coord.startswith("lon"): return "longitude"
    if coord.startswith("sigma"): return "sigma"
    if coord.startswith("rho_level_height"): return "level_height"
    if coord.startswith("theta_level_height"): return "level_height"
    if coord.startswith("time"): return "time"
    if coord.startswith("height"): return "height"
    return coord

def printwarn(cube,da,msg):
    print(f"Warning: [{cube.name()} <-> {da.name}]: {msg}",file=sys.stderr)

def printerr(cube,da,msg):
    print(f"ERROR: [{cube.name()} <-> {da.name}]: {msg}",file=sys.stderr)

def float_arr_compare(a,b):
    return (abs(a-b) <= np.maximum(tol*np.maximum(abs(a),abs(b)),eps) ).all()

def check_coords(cube,dataarray):
    warn_count=0
    err_count=0
    cube_coords_seen=[]
    #for da_coord in dataarray.dims:
    for da_coord in dataarray.coords:
        cube_coord=coord_translate(da_coord)
        ### The data arrays have more coordinates than the cubes, as um2netcdf4 sticks
        ### every scalar coord to every field. So if the coords are scalar, we can skip
        ### them if they're not present.
        try:
            cube_coord_data=cube.coord(cube_coord).points
        except iris.exceptions.CoordinateNotFoundError:
            if dataarray.coords[da_coord].size != 1:
                printerr(cube,dataarray,f"Coordinate {cube_coord} translated from {da_coord} not present in original UM data or")
                printerr(cube,dataarray,f"Multiple coordinates matching {cube_coord}, translated from {da_coord} found")
                err_count+=1
            else:
                printwarn(cube,dataarray,f"Scalar coord {da_coord} missing from original UM data")
                warn_count+=1
            continue
        cube_coords_seen.append(cube_coord)
        
        if da_coord.startswith("time"):
            ### Convert to cftime
            try:
                dataarray_coord=dataarray.convert_calendar("gregorian",dim=da_coord,use_cftime=True).coords[da_coord]
            except ValueError:
                ### Something dodgy about time_2
                dataarray_coord=dataarray.coords[da_coord]
        else:
            dataarray_coord=dataarray.coords[da_coord]
        
        if cube_coord=="time":
            all(np.array([ cube.coord(cube_coord).units.num2date(i) for i in cube_coord_data],dtype="object")==dataarray_coord)
        elif cube_coord_data.dtype == "float":
            if cube_coord=="pressure":
                ### This gets reversed:
                cube_coord_data=np.flipud(100.0*cube_coord_data)
            #if not all(abs(cube_coord_data-dataarray_coord.values) < tol):
            if not float_arr_compare(cube_coord_data,dataarray_coord.values):
                ### Are there any other coords that might match this one?
                coord_mismatch=[ True, ]
                for c in dataarray.coords:
                    if c.startswith(cube_coord) and c != dataarray_coord:
                        coord_mismatch.append(not float_arr_compare(cube_coord_data,dataarray.coords[c].values) )
                        if not coord_mismatch[-1]:
                            printwarn(cube,dataarray,f"Scalar coord {cube_coord} derived from {da_coord} matches da coord of different name: {c}")
                            warn_count+=1
                if all(coord_mismatch):
                    printerr(f"Difference between equivalent coordinates {cube_coord} and {da_coord} exceeds tolerance {tol}")
                    err_count+=1
        else:
            if not all(cube_coord_data == dataarray_coord.values):
                ### Are there any other coords that might match this one?
                coord_mismatch=[ True, ]
                for c in dataarray.coords:
                    if c.startswith(cube_coord) and c != dataarray_coord:
                        coord_mismatch.append(not all(cube_coord_data == dataarray.coords[c].values) )
                if all(coord_mismatch):
                    printerr(cube,dataarray,f"Difference between equivalent integer coordinates {cube_coord} and {da_coord}")
                    err_count+=1
            
    for name in [ i.name() for i in cube.coords() ]:
        if name == "forecast_period": continue
        if name == "forecast_reference_time": continue
        if name not in cube_coords_seen:
            printerr(cube,dataarray,f"Coordinate {name} not found in converted netCDF field")
            err_count+=1

    return (warn_count,err_count)


def check_field(cube,dataarray,err_count):
    if cube.data.dtype in ("float","float32"):
        #if not (abs(cube.data - dataarray.data) <= abs(cube.data*tol)).all():
        if cube.coords()[0].name()=="pseudo_level":
            ### Dims are going to be in the wrong order - transpose the dataarray
            da_dat=dataarray.transpose('pseudo_level',*[i for i in dataarray.dims if i != "pseudo_level" ])
        elif "pressure" in [ c.name() for c in cube.coords() ]:
            da_dat=dataarray.isel(pressure=slice(None,None,-1)).data
        else:
            da_dat=dataarray.data
        if not float_arr_compare(cube.data,da_dat):
            printerr(cube,dataarray,f"Field values {cube.name()} and {dataarray.name} differ by larger than tolerance")
            err_count+=1
    else:
        if not (cube.data == dataarray.data).all():
            printerr(cube,dataarray,f"Field values {cube.name()} and {dataarray.name} differ")
            err_count+=1
        
    return err_count


def get_ncfld(cube,ds):
    ### First guess...
    ncfld_name="fld_"+str(cube.attributes['STASH'])[3:]
    ### Is there a cell method?
    try:
        cube_method=cube.cell_methods[0].method
    except:
        ### Nope, lets be on our way
        cube_method = None
    ### Does the cube method match the xarray method?
    try:
        da_method=ds[ncfld_name].cell_methods.split(' ')[1]
    except:
        ### No cell method, we're done
        da_method = None
    
    if cube_method==da_method:
        return ds[ncfld_name]

    ### If we make it here, the cell methods don't match.
    ### Make a list of all fields in the dataset with a matching stash code
    for da in [ ds[i] for i in ds if i.startswith(ncfld_name) ]:
        try:
            da_method=da.cell_methods.split(' ')[1]
        except:
            continue
        if da_method == cube_method:
            return da
    
    return None

def verify_cube(cube_field,ncfld):
    #client=Client(scheduler_file=os.environ['SCHED_FILE'])
    print(f"Comparing UM output {cube_field.name()} and netCDF output {ncfld.name}",file=sys.stderr)
    try:
        warn,err=check_coords(cube_field,ncfld)
        err=check_field(cube_field,ncfld,err)
    except KeyError:
        raise

    return (warn,err)

if __name__ == "__main__":

    um_file, nc_file=sys.argv[1].split('+')

    try:
        umf=iris.load(um_file)
    except OSError:
        print(f"Warning: UM File missing",file=sys.stderr)
        print(f"Summary for {um_file} <-> {nc_file}: 1 Warnings, 0 Errors.")
        sys.exit(0)
    try:
        ds=xr.open_dataset(nc_file)
    except:
        print(f"Error: netCDF File {nc_file} missing",file=sys.stderr)
        print(f"Summary for {um_file} <-> {nc_file}: 0 Warnings, 1 Errors.")
        sys.exit(1)

    seen_nc_fields=[]
    verify_cube_args=[]
    for cube_field in umf:
        #ncfld_name="fld_"+str(cube_field.attributes['STASH'])[3:]
        ncfld=get_ncfld(cube_field,ds)
        seen_nc_fields.append(ncfld.name)
        verify_cube_args.append((cube_field,ncfld))

    with Pool(min(3,len(verify_cube_args))) as pool:
        out=pool.starmap(verify_cube,verify_cube_args)
    
    warn_count,err_count=tuple(sum(i) for i in zip(*out))

    for name in [ i for i in ds if i.startswith('fld_') ]:
        if name not in seen_nc_fields:
            print(f"netCDF field {name} not found in UM output",file=sys.stderr)
            err_count+=1

    print(f"Summary for {um_file} <-> {nc_file}: {warn_count} Warnings, {err_count} Errors.")
    if err_count > 0:
        sys.exit(1)