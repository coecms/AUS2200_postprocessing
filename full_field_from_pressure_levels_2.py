import datetime
import os,sys
import iris
import xarray as xr
import warnings
import numpy as np
from scipy.interpolate import interp1d
from dask.distributed import Client
import argparse

def plev19():
    plev19=np.array([100000, 92500, 85000, 70000,
        60000, 50000, 40000, 30000,
        25000, 20000, 15000, 10000,
        7000, 5000, 3000, 2000,
        1000, 500, 100],dtype=np.float32)
    return np.flip(plev19)

def pointwise_interp(pres, var, plev):
    vint = interp1d(pres,var,kind="linear",fill_value="extrapolate")
    return vint(plev)

parser = argparse.ArgumentParser()
parser.add_argument("-s","--stashcode")
parser.add_argument("-p","--fileprefix")
parser.add_argument("-r","--dorho",action='store_true')
parser.add_argument("-g","--regrid",action='store_true')
parser.add_argument("-f","--frequency",default=1,type=int)
ns = parser.parse_args()

warnings.simplefilter(action='ignore', category=FutureWarning)
np.set_printoptions(threshold=sys.maxsize)
var_order=["time","pressure_level","latitude","longitude"]

client=Client('127.0.0.1:8786')

file_prefix="umnsa_mdl_"
starttime=datetime.datetime.fromisoformat('2022-02-22T00:00:00')
#endtime=datetime.datetime.fromisoformat('2022-02-22T01:00:00')
endtime=datetime.datetime.fromisoformat('2022-03-07T19:00:00')

file_d=starttime
dir_d=starttime
file_td=datetime.timedelta(hours=ns.frequency)
dir_td=datetime.timedelta(hours=6)
iso_dir_d=dir_d.strftime("%Y%m%dT%H00")
iso_file_d=file_d.strftime("%Y%m%dT%H00")

# pressure - m01s00i408
if ns.dorho:
    pressure_con=iris.AttributeConstraint(STASH='m01s00i407')
else:
    pressure_con=iris.AttributeConstraint(STASH='m01s00i408')
temp_con=iris.AttributeConstraint(STASH=ns.stashcode)
pressure_cubes=[]
temp_cubes=[]

counter=24
dayno=1

print(sys.argv)

plev=plev19()

datasets=[]
filenames=[]

while dir_d < endtime:
    iso_dir_d=dir_d.strftime("%Y%m%dT%H00")
    iso_file_d=file_d.strftime("%Y%m%dT%H00")
        
    print(iso_file_d + " " + iso_dir_d)

    pressure_cubes=[]
    temp_cubes=[]
        
    pc=iris.load(f'/g/data/hh5/tmp/WACI-Hackathon-2023/AUS2200/day{dayno}/{iso_dir_d}Z/aus2200/d0198/RA3/um/{file_prefix}{iso_file_d}',pressure_con)[0]
    if ns.dorho: 
        pc=pc[:70]
    tc=iris.load(f'/g/data/hh5/tmp/WACI-Hackathon-2023/AUS2200/day{dayno}/{iso_dir_d}Z/aus2200/d0198/RA3/um/{file_prefix}{iso_file_d}',temp_con)[0]
        
    pc.remove_coord('forecast_period')
    pc.remove_coord('forecast_reference_time')
    tc.remove_coord('forecast_period')
    tc.remove_coord('forecast_reference_time')

    ## For ua nd v components of wind
    if ns.regrid:
        tc=tc.regrid(pc,iris.analysis.Linear())

    if ns.stashcode == "m01s15i101":
        ### Not sure this is right but what else can I do?
        for tc_i in range(len(tc.coords())):
            if tc.coords()[tc_i].name() == "time": break
        for pc_i in range(len(tc.coords())):
            if pc.coords()[pc_i].name() == "time": break

        tc.coords()[tc_i].points=pc.coords()[pc_i].points


    pda=xr.DataArray.from_iris(pc).chunk({"model_level_number":-1,"latitude":106,"longitude":130}).expand_dims('time')
    tda=xr.DataArray.from_iris(tc).chunk({"model_level_number":-1,"latitude":106,"longitude":130}).expand_dims('time')

    print(pda)
    print(tda)

    filenames.append(f"/scratch/v45/dr4292/pressure_levels/{ns.fileprefix}_on_pressure_levels_{iso_file_d}.nc")

    interp=xr.apply_ufunc(
        pointwise_interp,
        pda,
        tda,
        plev,
        input_core_dims=[ ["model_level_number"],["model_level_number"], ["pressure_level"]],
        output_core_dims=[ ["pressure_level"] ],
        exclude_dims=set(("model_level_number",)),
        vectorize=True,
        dask="parallelized",
        output_dtypes=['float32']
    )

    interp['pressure_level']=plev
    interp=interp.transpose(*var_order)
    interp.name=tc.name()

    datasets.append(interp.to_dataset())

    #interp.to_netcdf("/scratch/v45/dr4292/test.nc")
    #print(filenames)
    #exit()

    counter=counter+ns.frequency
    file_d=file_d+file_td
    if counter > 24 and counter%6 == 0:
        dir_d=dir_d+dir_td
    if counter > 24 and counter%24 == 0:
        dayno=dayno+1


#interp.to_netcdf("/scratch/v45/dr4292/test2.nc")
xr.save_mfdataset(datasets,filenames)
