from neuron_getter import NeuronRetriever
import trimesh
from neuron_check_mesh import NeuronChecker
import pandas as pd 
from tqdm import tqdm
import struct
import sys
import numpy as np
import os

NEURON_GETTER = NeuronRetriever()
NEURON_CHECKER = NeuronChecker()
WRITE_RATE = 20


def combine_mesh(neuron_class, nid, commit=False):
    print(f'Combining {nid}')
    fpth = os.path.join('/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh', neuron_class)
    mesh_list = NEURON_GETTER.retrieve_neuron(nid)
    combined_trimesh = trimesh.util.concatenate(mesh_list)
    b_file = trimesh_to_binary(combined_trimesh)
    os.makedirs(fpth, exist_ok=True)
    print('Writing to file ...')
    with open(os.path.join(fpth, nid), 'wb') as f:
        f.write(b_file)
    NEURON_CHECKER.update_neuron(neuron_class, nid, True, commit=commit)


def trimesh_to_binary(trimesh_obj):
    triangles = np.array(trimesh_obj.faces).flatten()
    vertices =  np.array(trimesh_obj.vertices)
    num_vertices = len(vertices)

    b_array = bytearray(4 + 4 * 3 * len(vertices) + 4 * len(triangles))
    print(f'to_binary num_vertices: {num_vertices}')
    struct.pack_into('<I', b_array, 0, num_vertices)
    print('Writing vertices ...')
    struct.pack_into('<' + 'f'*len(vertices.flatten()),
                     b_array,
                     4,
                     *vertices.flatten())
    print('Writing faces ...')
    struct.pack_into('<' + 'I'*len(triangles),
                     b_array,
                     4 + 4 * 3 * num_vertices,
                     *triangles)
    return b_array


#############################################################################


def test1():
    nid = 'grc_100'
    mesh_list = NEURON_GETTER.retrieve_neuron(nid)
    combined_trimesh = trimesh.util.concatenate(mesh_list)

    bf = trimesh_to_binary(combined_trimesh)
    fpth = '/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh'
    with open(os.path.join(fpth, nid), 'wb') as f:
        f.write(bf)


def main():
    neuron_class = 'grc'
    try:
        if len(sys.argv) == 1:
            'pass'
        elif len(sys.argv) == 2:
            neuron_class = sys.argv[1]
    except Exception as e:
        print(e)
        print("Unable to get parameter correctly.")
        exit(1)

    neurons_need_merge = NEURON_CHECKER.get_untested_neurons(neuron_class)
    pbar = tqdm(total=len(neurons_need_merge), desc='MERGING NEURONS')
    for idx, nid in enumerate(neurons_need_merge):
        try:
            if idx % WRITE_RATE == 1:
                combine_mesh(neuron_class, nid, commit=True)
            else:
                combine_mesh(neuron_class, nid, commit=False)
            pbar.update(1)
        except AssertionError as e:
            print(e)
            print('MESH RETRIEVE FAILED, SKIP: {}'.format(nid))
            pbar.update(1)
            continue
    pbar.close()
    NEURON_CHECKER.commit_to_db()


if __name__ == "__main__":
    # main()
    test1()