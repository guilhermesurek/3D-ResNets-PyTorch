import urllib3, json
import os
import pandas as pd
import numpy as np
import math
from pathlib import Path

def get_annotation_file(dst=None, version='1.3'):
    if version == '1.3':
        # Get ActivityNet Annotation file for v1.3 and save it in a json file
        url = "http://ec2-52-25-205-214.us-west-2.compute.amazonaws.com/files/activity_net.v1-3.min.json"
        response = urllib3.PoolManager().request('GET', url).data
        data = json.loads(response)
        # Prepare destination path
        if dst == None:
            dst = os.path.dirname(os.path.realpath(__file__))
        dst_path = os.path.join(dst, 'activity_net.v1-3.min.json')
        with open(dst_path, 'w') as outfile:
            json.dump(data, outfile)
    else:
        raise KeyError(f"Version {version} not configured in function get_activitynet_annotation_file.")

def rename_df_col(col):
    if isinstance(col, tuple):
        col = '_'.join(str(c) for c in col)
    return col

def annotation_stats(src):
    ###Create dataframe with annotation info
    annotation_path = Path(src)
    with annotation_path.open('r') as f:
        data = json.load(f)
    # Define columns
    cols = ['video_id', 'url', 'subset', 'resolution', 'fps', 'duration', 'label', 'segment_low', 'segment_high', 'frame_low', 'frame_high', 'frame_qty']
    # Create empty DF
    df_ann = pd.DataFrame(columns=cols)
    # Loop over annotation file
    i = 0       # Counter
    total = len(data['database'].keys())
    for video_id in data['database'].keys():
        i+=1                                        # Increment counter
        print(f"Loop {i}/{total}: {video_id}")      # print info
        # Prepare base vec
        try:
            base_vec = {'video_id': video_id, 
                        'url': data['database'][video_id]['url'],
                        'subset': data['database'][video_id]['subset'],
                        'resolution': data['database'][video_id]['resolution'],
                        'fps': data['database'][video_id]['fps'],
                        'duration': data['database'][video_id]['duration']}
            # Loop over annotations of the video_id
            for ann in data['database'][video_id]['annotations']:
                # Add info to the final vec, that will be append to the dataframe
                final_vec = base_vec
                final_vec['label'] = ann['label']
                final_vec['segment_low'] = ann['segment'][0]
                final_vec['segment_high'] = ann['segment'][1]
                final_vec['frame_low'] = math.floor(ann['segment'][0] * base_vec['fps']) + 1
                final_vec['frame_high'] = math.floor(ann['segment'][1] * base_vec['fps']) + 1
                final_vec['frame_qty'] = (final_vec['frame_high']-final_vec['frame_low']) if (final_vec['frame_high']-final_vec['frame_low']) > 8 else 0
                # Append to the dataframe
                df_ann = df_ann.append(final_vec, ignore_index=True)
        except KeyError as e:
            print(f"KeyError in Video: {video_id} Key: {e}")
    # Generate annotation stats
    print("Generating annotation stats")
    df_res = df_ann.groupby(['subset', 'video_id', 'label']).agg({'fps':'mean', 'duration':'mean', 'frame_qty': 'sum'}).groupby(['subset', 'label']).agg({'fps':'mean', 'duration':'mean', 'frame_qty': ['sum', 'count']})
    df_res[('frame_qty', 'mean')] = df_res[('frame_qty',   'sum')]/df_res[('frame_qty', 'count')]
    df_res.columns = map(rename_df_col, df_res.columns)
    df_res.reset_index(inplace=True)
    # Save in excel file
    print("Saving stats in excel file")
    dst = os.path.dirname(os.path.realpath(__file__))
    dst_path = os.path.join(dst, 'annotation_stats.xlsx')
    df_res.to_excel(dst_path)
    # Plot distribuitions
    print("Training set's video count distribuition over classes")
    df_res[df_res['subset']=='training']['frame_qty_count'].plot.kde()
    print("Validation set's video count distribuition over classes")
    df_res[df_res['subset']=='validation']['frame_qty_count'].plot.kde()
    print("Training set's frame count distribuition over classes")
    df_res[df_res['subset']=='training']['frame_qty_mean'].plot.kde()
    print("Validation set's frame count distribuition over classes")
    df_res[df_res['subset']=='validation']['frame_qty_mean'].plot.kde()