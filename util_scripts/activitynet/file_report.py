import os
import pandas as pd
import timeit

start = timeit.default_timer()
dir_path = 'E:/Mestrado/dataset/act200/test'
#dir_path = 'D:/Mestrado/Dataset/dst/activitynet200/test'

if __name__ == "__main__":
    c=0
    # List video's folders
    file_list = list(sorted(os.listdir(dir_path)))
    # Create empty df
    df = pd.DataFrame(columns=['video_id', 'qty_files'])
    for eFile in file_list:
        # Make subfolder path
        mk_path = os.path.join(dir_path, eFile)
        # Count files in it
        files_number = len(os.listdir(mk_path))
        # Save in dict
        aux = {
            'video_id': eFile,
            'qty_files': files_number
        }
        # append to df
        df = df.append(aux, ignore_index=True)
        #print(f"Folder {eFile} has {files_number} files in it.")
        c+=1
        print(f"c:{c}")
    # Make excel dst path
    dst = os.path.dirname(os.path.realpath(__file__))
    dst_path = os.path.join(dst, 'output.xlsx')
    df.to_excel(dst_path)
    # Print head
    stop = timeit.default_timer()
    print('Time: ', stop - start)