import pandas as pd
import pickle
import os
import numpy as np
import argparse
from tqdm import tqdm

data_file_name = "alexandras_outway_traj_merged_lane.csv"
data_path = "/home/data/data/pneuma/20181029_D8_1000_1030/"


def data_generator(data_path, timestamp_period):
    # read original data
    data = pd.read_csv(data_path)

    # columns to remain
    col_li = ['track_id', 'speed', 'lon_acc', 'lat_acc', 'time', 'type', 
            'moving', 'img_x', 'img_y', 'img_angle', 'coord_id', 'Lane']

    # leave columns that are needed
    data = data[col_li]

    # if mean of Lane is not 0, then lane change occured
    g = data.groupby(['track_id']).agg({'Lane': 'mean'})%1

    lc = g.loc[g.Lane != 0]

    # get list of track id with lane change occured
    lc_true_track_id = lc.index.unique()

    # get dataframe with lc true
    lc_tracks = data[data['track_id'].isin(lc_true_track_id)]

    # adding trigger column by groups (when lc occured)
    lc_tracks['trigger'] = lc_tracks.groupby('track_id', group_keys=False).apply(lambda x: x['Lane'].diff().fillna(0))

    # change trigger to boolean type
    lc_tracks['trigger'] = lc_tracks['trigger'].astype('bool')


    folder_name = "timestamp_period_" + str(timestamp_period)
    os.makedirs(folder_name, exist_ok=True)

    path = os.path.join(os.path.abspath("."), folder_name)

    total_count = 0

    for id in tqdm(lc_tracks.track_id.unique(), leave=True, desc="track id processing"):
        count = 1 # for csv file name

        items = lc_tracks.loc[lc_tracks.track_id == id]

        idx = items.loc[items.trigger == True].index


        for i in idx:

            try:
                items.loc[i - timestamp_period]
                items.loc[i + timestamp_period]

            except Exception as e:
                continue

            else:
                filename = str(id) + "_lc" + '{0:02d}'.format(count) + '.csv'
                # print(filename)

                parts = items.loc[i - timestamp_period: i + timestamp_period]

                # save in csv
                parts.to_csv(os.path.join(path, filename), mode='w')
                count+=1
                total_count+=1

    print(f"{total_count} data was created")
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Argparse Tutorial')

    # 입력받을 인자값 설정 (default 값 설정가능)
    parser.add_argument('--timestamp_period',          type=int)
    parser.add_argument('--data_path',     type=str,   default=data_path + data_file_name)
    
    # args 에 위의 내용 저장
    args    = parser.parse_args()

    # print(args.timestamp_period)
    # print(args.data_path)
    data_generator(args.data_path, args.timestamp_period)