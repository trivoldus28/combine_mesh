from neuron_getter import NeuronRetriever
import trimesh
from neuron_check_mesh import NeuronChecker
import pandas as pd 
from tqdm import tqdm
import struct
import sys
import numpy as np
import os
from multiprocessing import Process, Manager

DEFAULT_PROCESS_NUM = 4
WRITE_RATE = 20
BINARY_MESH_PATH = '/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh'

MANUAL = """
Welcome to combine mesh.

"""

class MeshCombiner:

    def __init__(
        self, 
        pymongo_path='/n/groups/htem/Segmentation/tmn7/segwaytool.proofreading/segwaytool/proofreading/',
        base_path='/n/f810/htem/Segmentation/cb2_v4/output.zarr/meshes/precomputed_v2/mesh/',
        db_name='neurondb_cb2_v4',
        db_host='mongodb://10.117.28.250:27018/',
        mesh_hierarchical_size=10000,
        daisy_block_id_add_one_fix=True,
        hierarchy_lut_path='/n/f810/htem/Segmentation/cb2_v4/output.zarr/luts/fragment_segment',
        super_lut_pre='super_1x2x2_hist_quant_50',
        neuron_checker_dir='/n/groups/htem/Segmentation/xg76/combine_mesh/neuron_check/neuron_mesh.db',
        dahlia_db_name='neurondb_cb2_v4',
        dahlia_db_host='mongodb://10.117.28.250:27018/',
        binary_mesh_path='/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh',
        write_rate=20,
        default_process_num=4):
        
        self.neuron_getter = NeuronRetriever(
            pymongoPath=pymongo_path, 
            basePath=base_path, 
            db_name=db_name,
            db_host=db_host,
            meshHierarchichal_size=mesh_hierarchical_size,
            daisy_block_id_add_one_fix=daisy_block_id_add_one_fix,
            hierarchy_lut_path=hierarchy_lut_path,
            super_lut_pre=super_lut_pre 
        )
        self.neuron_checker = NeuronChecker(
            db_dir=neuron_checker_dir,
            dahlia_db_name=dahlia_db_name,
            dahlia_db_host=dahlia_db_host
        )
        self.write_rate = write_rate,
        self.binary_mesh_path = binary_mesh_path
        self.default_proess_num = default_process_num


    def __combine_mesh(self, nid):
        mesh_list = self.neuron_getter.retrieve_neuron(nid)
        combined_trimesh = trimesh.util.concatenate(mesh_list)
        b_file = trimesh_to_binary(combined_trimesh)
        os.makedirs(self.binary_mesh_path, exist_ok=True)
        # print('Writing to file ...')
        with open(os.path.join(self.binary_mesh_path, nid), 'wb') as f:
            f.write(b_file)

    
    def combine_mesh(self, nid, process_num=-1):
        if isinstance(nid, str):
            nid = [nid]
        process_num = self.default_proess_num if process_num < 0 else process_num
        
        if process_num > 1:
            try:
                n_list = Manager().list()
                jobs = []
                neuron_lists = [nid[i:i+self.process_num] for i in range(0, len(nid), self)]
                for neurons in neuron_lists:
                    jobs.append(Process(target=self.__combine_mesh, args=(neurons,)))
                for j in jobs: j.start()
                for j in jobs: j.join()
            except Exception as e:
                print(e)
                print("switch to single thread")
                for n in nid: self.__combine_mesh(n)
        else:
            for n in nid: self.__combine_mesh(n)
        
        self.neuron_checker.commit_to_db()






def combine_mesh_list(nid_list, process_num=DEFAULT_PROCESS_NUM, commit=False):
    if process_num <= 1 :
        p_list = Manager().list()
        jobs = []


def combine_mesh(nid, commit=False):
    # print(f'Combining {nid}')
    fpth = BINARY_MESH_PATH
    mesh_list = NEURON_GETTER.retrieve_neuron(nid)
    combined_trimesh = trimesh.util.concatenate(mesh_list)
    b_file = trimesh_to_binary(combined_trimesh)
    os.makedirs(fpth, exist_ok=True)
    # print('Writing to file ...')
    with open(os.path.join(fpth, nid), 'wb') as f:
        f.write(b_file)


def trimesh_to_binary(trimesh_obj):
    triangles = np.array(trimesh_obj.faces).flatten()
    vertices =  np.array(trimesh_obj.vertices)
    num_vertices = len(vertices)

    b_array = bytearray(4 + 4 * 3 * len(vertices) + 4 * len(triangles))
    # print(f'to_binary num_vertices: {num_vertices}')
    struct.pack_into('<I', b_array, 0, num_vertices)
    # print('Writing vertices ...')
    struct.pack_into('<' + 'f'*len(vertices.flatten()),
                     b_array,
                     4,
                     *vertices.flatten())
    # print('Writing faces ...')
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
    print(f'{neuron_class} has {len(neurons_need_merge)} unmerged.')
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
    main()