import netCDF4 as nc
import pandas as pd
import h3
from tqdm import tqdm
from collections import defaultdict
from fishtank.dimensions.spatial import H3_RESOLUTIONS
import multiprocessing as mp
import numpy as np


def get_grouped_info(args):
    elevation, lats, lons = args
    totals = defaultdict(int)
    counts = defaultdict(int)
    for i in tqdm(range(len(lats))):
        for j in range(len(lons)):
            h3_index = h3.geo_to_h3(lats[i], lons[j], max(H3_RESOLUTIONS))
            totals[h3_index] += elevation[i, j]
            counts[h3_index] += 1
    return totals, counts


def get_split_data(elevation, lats, lons, n):
    split_size = int(np.ceil(len(lats) / n))
    splits = [
        (elevation[i : i + split_size], lats[i : i + split_size], lons)
        for i in range(0, len(lats), split_size)
    ]
    return splits


if __name__ == "__main__":
    print("loading data...")
    prefix = "data/gebco_2023_n78.9532_s34.4614_w160.5624_e237.5031"
    dataset = nc.Dataset(f"{prefix}.nc")

    elevation = dataset["elevation"][:]
    lats = dataset["lat"][:]
    lons = dataset["lon"][:]

    print("grouping data...")
    num_processes = 8
    with mp.Pool(num_processes) as p:
        results = p.map(
            get_grouped_info, get_split_data(elevation, lats, lons, num_processes)
        )

    print("accumulating data...")
    totals = results[0][0]
    counts = results[0][1]
    for new_totals, new_counts in results[1:]:
        for key in new_totals:
            totals[key] += new_totals[key]
            counts[key] += new_counts[key]

    print("converting to dataframe...")
    dataframe = pd.DataFrame(
        [
            {
                "lat": h3.h3_to_geo(key)[0],
                "lon": h3.h3_to_geo(key)[1],
                "elevation": totals[key] / counts[key],
            }
            for key in totals
        ]
    )
    print("writing to csv...")
    dataframe.to_csv(f"{prefix}.csv", index=False)
