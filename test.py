import struct
import numpy as np
import shutil
import os


def test1():
    v = np.array(
        [[540000, 360000, 26800],
         [520000, 350000, 25000],
         [560000, 340000, 27000],
        ])
    f = np.array([[0,1,2]])
    num_v = 3
    with open('/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh/test', 'wb') as file:
        struct.pack_into('<I', file, 0, num_v)
        struct.pack('<fffffffff', file, 4, *v.flatten())
        struct.pack('<III', file, 40, *f.flatten())
        # vn = struct.pack('<I', num_v)
        # vertices = struct.pack('<fffffffff', *v.flatten())
        # faces = struct.pack('<' + 'III', *f.flatten())
    # with open('/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh/tst', 'wb') as file:
    #     file.write(vn+vertices+faces)


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