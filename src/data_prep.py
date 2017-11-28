import os
import pickle
from collections import namedtuple

import feather
import pandas as pd
from geopy.geocoders import Nominatim
from tqdm import tqdm

tqdm.pandas()

coords = namedtuple('coords', 'latitude, longitude')

project_dir = os.path.join(os.getcwd(), os.pardir)
data_path = os.path.join(project_dir, 'data', 'ZeeMap-2787497.csv')

df = pd.read_csv(data_path)

geodict_path = os.path.join(project_dir, 'data', 'geodict.p')
geodict = pickle.load(open(geodict_path, 'rb'))

geolocator = Nominatim()


def get_latitude(x):
    return x.latitude


def get_longitude(x):
    return x.longitude


def get_geocode(loc):
    # dict.get will run default value if a function, regardless of existance of key
    value = geodict.get(loc, False)
    if value:
        return value
    else:
        return geolocator.geocode(loc)


df['Area_Name'] = df['City'].astype(str) + ', ' + df['State'].astype(str) + ', ' + df['Country'].astype(str).replace(
    'nan, ', '')
df['Area_Name'] = df['Area_Name'].str.replace('nan, ', '')

geolocate_column = df['Area_Name'].progress_apply(get_geocode)
df['latitude'] = geolocate_column.apply(get_latitude)
df['longitude'] = geolocate_column.apply(get_longitude)

geodict_out = {it.Area_Name: coords(it.latitude, it.longitude) for it in df.itertuples()}
with open(geodict_path, 'wb') as fp:
    print(f'Writing file: {geodict_path}')
    pickle.dump(geodict_out, fp)

df = df.loc[:, ['Name', 'latitude', 'longitude', 'Description']]

feather_path = os.path.join(project_dir, 'data', 'geocoded_df.feather')
csv_out_path = os.path.join(project_dir, 'data', 'geocoded_df.csv')
print(f'Writing file: {feather_path}')
feather.write_dataframe(df, feather_path)
df.to_csv(csv_out_path)
