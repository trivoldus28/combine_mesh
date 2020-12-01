import struct
import numpy as np
import shutil
import os


def copy():
    path = '/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh_catg'
    dest_path = '/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh'
    neurons = os.listdir(path)
    for n_class in neurons:
        n_list = os.listdir(os.path.join(path, n_class))
        print(f"Copy {n_class} ...")
        for n in n_list:
            shutil.copyfile(os.path.join(path, n_class, n), os.path.join(dest_path, n))



if __name__ == "__main__":
    copy()