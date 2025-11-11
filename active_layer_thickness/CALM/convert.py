import os
import time

import cftime as cf
import numpy as np
import pandas as pd
import xarray as xr

# Get data
remote_source = "https://www2.gwu.edu/~calm/data/CALM_Data/CALM_Summary_table.xls"
local_source = os.path.basename(remote_source)
if not os.path.isfile(local_source):
    os.system(f"wget {remote_source}")

# Timestamps
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getctime(local_source))
)
generate_stamp = time.strftime("%Y-%m-%d")

# Find a way to search the excel sheet for these
dfs = []
for first, last in zip(
    [
        31,
        76,
        103,
        108,
        147,
        156,
        182,
        199,
        228,
        237,
        244,
        250,
        254,
        261,
        268,
        273,
        289,
        339,
        353,
    ],
    [
        72,
        99,
        104,
        142,
        152,
        178,
        195,
        224,
        233,
        239,
        246,
        250,
        257,
        264,
        269,
        284,
        335,
        349,
        355,
    ],
):
    dfs.append(pd.read_excel(local_source, skiprows=first - 2, nrows=last - first + 1))
df = pd.concat(dfs)

# Fix some column names
df = df.reset_index(drop=True)
df = df.rename(
    columns={
        "Unnamed: 0": "Site Code",
        "Unnamed: 1": "Site Name",
        "Unnamed: 4": "Method",
        "###": 1992,
        "###.1": 1993,
        "###.2": 1994,
        "###.3": 1995,
        "###.4": 1997,
    }
)

# Mistake in the source data
query = df[df["Site Name"] == "Andryushkino"]
assert len(query) == 1
if query["LAT"].iloc[0] < 60:
    df.loc[df["Site Name"] == "Andryushkino", "LAT"] += 60.0

# Cleanup the data columns
years = [c for c in df.columns if isinstance(c, int)]
for year in years:
    col = df[year].astype(str).str.strip()
    col = col.replace(["-", "inactive"], np.nan)
    col = col.str.replace("*", "", regex=False)
    col = col.str.replace("<", "", regex=False)
    col = col.str.replace(">", "", regex=False)
    col = col.replace("", np.nan)
    df[year] = col.astype(float)

df = df[~df[years].isna().all(axis=1)].reset_index(drop=True)

tb = np.array(
    [
        [cf.DatetimeNoLeap(y, 1, 1) for y in years],
        [cf.DatetimeNoLeap(y + 1, 1, 1) for y in years],
    ]
).T
t = np.array([tb[i, 0] + 0.5 * (tb[i, 1] - tb[i, 0]) for i in range(tb.shape[0])])
start_year = years[0]
end_year = years[-1]
ds = xr.DataArray(
    df[years].to_numpy().T,
    coords={"time": t},
    dims=("time", "sites"),
    attrs={"long_name": "Average thaw depth at end-of-season", "units": "cm"},
).to_dataset(name="alt")
ds["site_code"] = xr.DataArray(df["Site Code"].to_numpy(), dims=("sites"))
ds["site_code"].attrs = {"long_name":"CALM site code"}
ds["site_name"] = xr.DataArray(df["Site Name"].to_numpy(), dims=("sites"))
ds["site_name"].attrs = {"long_name":"CALM site name"}
ds["time_bnds"] = xr.DataArray(tb, dims=("time", "nb"))
ds["lat"] = xr.DataArray(df["LAT"].to_numpy(), dims=("sites"))
ds["lon"] = xr.DataArray(df["LONG"].to_numpy(), dims=("sites"))
ds.attrs = {
    "title": "CALM: Circumpolar Active Layer Monitoring Network",
    "versions": "2022",
    "institutions": "The George Washington University",
    "source": remote_source,
    "history": f"Downloaded on {download_stamp} and generated netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/ILAMB-Data/blob/master/CALM/convert.py",
    "references": """
@ARTICLE{CALM,
  author = {CALM},
  title = {Circumpolar Active Layer Monitoring Network-CALM: Long-Term Observations of the Climate-Active Layer-Permafrost System.},
  journal = {online},
  url = {https://www2.gwu.edu/~calm/}
}""",
}
ds.to_netcdf(
    f"CALM_{start_year}-{end_year}.nc",
    encoding={
        "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
        "time_bnds": {"units": "days since 1850-01-01"},
    },
)
