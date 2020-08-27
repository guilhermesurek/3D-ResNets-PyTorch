import subprocess
import argparse
from pathlib import Path
import glob
import os
import pandas as pd

from joblib import Parallel, delayed

counter = 0

def list_files_in_dir(dir_path, ext=None, only_filename=False, only_filename_without_ext=False):
    if ext == None:
        if not only_filename and not only_filename_without_ext:
            files_list = glob.glob("%s/*" % dir_path)
        elif only_filename:
            files_list = [os.path.basename(x) for x in glob.glob("%s/*" % dir_path)]
        elif only_filename_without_ext:
            files_list = [os.path.splitext(os.path.basename(x))[0] for x in glob.glob("%s/*" % dir_path)]
    else:
        if not only_filename and not only_filename_without_ext:
            files_list = glob.glob("%s/*%s" % (dir_path, ext))
        elif only_filename:
            files_list = [os.path.basename(x) for x in glob.glob("%s/*%s" % (dir_path, ext))]
        elif only_filename_without_ext:
            files_list = [os.path.splitext(os.path.basename(x))[0] for x in glob.glob("%s/*%s" % (dir_path, ext))]
    return files_list

def get_video_file_paths_not_processed(dir_path, dst_path):
    video_file_paths_not_processed = []
    video_file_paths = [[os.path.splitext(os.path.basename(x))[0], x] for x in sorted(dir_path.iterdir())]
    video_file_paths_processed = [x for x in sorted(list_files_in_dir(dst_path, only_filename_without_ext=True))]
    if len(video_file_paths_processed) == 0:
        return [y for [x,y] in video_file_paths]
    dfA = pd.DataFrame(video_file_paths)
    dfB = pd.DataFrame(video_file_paths_processed)
    df = pd.merge(dfA, dfB, on=[0], how="outer", indicator=True
              ).query('_merge=="left_only"')
    video_file_paths_not_processed = df[1].to_list()
    return video_file_paths_not_processed

def video_process(video_file_path, dst_root_path, ext, fps=-1, size=240):
    counter =+ 1
    print(f"---Video Counter: {counter} | {os.path.splitext(os.path.basename(video_file_path))[0]}")
    if ext != video_file_path.suffix:
        return

    ffprobe_cmd = ('ffprobe -v error -select_streams v:0 '
                   '-of default=noprint_wrappers=1:nokey=1 -show_entries '
                   'stream=width,height,avg_frame_rate,duration').split()
    ffprobe_cmd.append(str(video_file_path))

    p = subprocess.run(ffprobe_cmd, capture_output=True)
    res = p.stdout.decode('utf-8').splitlines()
    if len(res) < 4:
        return

    frame_rate = [float(r) for r in res[2].split('/')]
    frame_rate = frame_rate[0] / frame_rate[1]
    duration = float(res[3])
    n_frames = int(frame_rate * duration)

    name = video_file_path.stem
    dst_dir_path = dst_root_path / name
    dst_dir_path.mkdir(exist_ok=True)
    n_exist_frames = len([
        x for x in dst_dir_path.iterdir()
        if x.suffix == '.jpg' and x.name[0] != '.'
    ])

    if n_exist_frames >= n_frames:
        return

    width = int(res[0])
    height = int(res[1])

    if width > height:
        vf_param = 'scale=-1:{}'.format(size)
    else:
        vf_param = 'scale={}:-1'.format(size)

    if fps > 0:
        vf_param += ',minterpolate={}'.format(fps)

    ffmpeg_cmd = ['ffmpeg', '-i', str(video_file_path), '-vf', vf_param]
    ffmpeg_cmd += ['-threads', '1', '{}/image_%05d.jpg'.format(dst_dir_path)]
    print(ffmpeg_cmd)
    subprocess.run(ffmpeg_cmd)
    print('\n')

def class_process(class_dir_path, dst_root_path, ext, fps=-1, size=240):
    if not class_dir_path.is_dir():
        return

    dst_class_path = dst_root_path / class_dir_path.name
    dst_class_path.mkdir(exist_ok=True)

    for video_file_path in sorted(class_dir_path.iterdir()):
        video_process(video_file_path, dst_class_path, ext, fps, size)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'dir_path', default=None, type=Path, help='Directory path of videos')
    parser.add_argument(
        'dst_path',
        default=None,
        type=Path,
        help='Directory path of jpg videos')
    parser.add_argument(
        'dataset',
        default='',
        type=str,
        help='Dataset name (kinetics | mit | ucf101 | hmdb51 | activitynet)')
    parser.add_argument(
        '--n_jobs', default=-1, type=int, help='Number of parallel jobs')
    parser.add_argument(
        '--fps',
        default=-1,
        type=int,
        help=('Frame rates of output videos. '
              '-1 means original frame rates.'))
    parser.add_argument(
        '--size', default=240, type=int, help='Frame size of output videos.')
    args = parser.parse_args()

    if args.dataset in ['kinetics', 'mit', 'activitynet']:
        ext = '.mp4'
    else:
        ext = '.avi'

    if args.dataset == 'activitynet':
        video_file_paths = [x for x in sorted(args.dir_path.iterdir())]
        # Get not processed video paths
        video_file_paths = get_video_file_paths_not_processed(args.dir_path, args.dst_path)
        status_list = Parallel(
            n_jobs=args.n_jobs,
            backend='threading')(delayed(video_process)(
                video_file_path, args.dst_path, ext, args.fps, args.size)
                                 for video_file_path in video_file_paths)
    else:
        class_dir_paths = [x for x in sorted(args.dir_path.iterdir())]
        test_set_video_path = args.dir_path / 'test'
        if test_set_video_path.exists():
            class_dir_paths.append(test_set_video_path)

        status_list = Parallel(
            n_jobs=args.n_jobs,
            backend='threading')(delayed(class_process)(
                class_dir_path, args.dst_path, ext, args.fps, args.size)
                                 for class_dir_path in class_dir_paths)
