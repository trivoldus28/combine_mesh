from neuron_getter import NeuronRetriever
import trimesh
from neuron_check_mesh import NeuronChecker
import pandas as pd
from tqdm import tqdm
import struct
import sys
import numpy as np
import os
import re
import json
from multiprocessing import Process, Manager

MANUAL = """
Welcome to combine mesh.

Please enter the [config_file_path]

The config file should be a json file containing databse configs and 
mode configs. The json file should have two objects: "database_config" 
and "mode_config". database_config might just be empty since all of the 
fields have default values.
Possible fields in database_config:
    -- pymongo_path
    -- base_path
    -- db_name
    -- db_host
    -- mesh_hierarchical_size
    -- daisy_block_id_add_one_fix
    -- hierarchy_lut_path
    -- super_lut_pre
    -- neuron_checker_dir
    -- dahlia_db_name
    -- dahlia_db_host
    -- binary_mesh_path
    -- default_process_num

Fields in mode_config:
    -- "mode" (mandatory): 
        There are two possible mode: "neuron_list" and "autocheck"
    -- "process_num" (optional):
        The value guides multiprocessing, default -1. 
        If < 0, use default value.
        If > 1, use multi processing strategy.
        Else, use single process.
    -- "include_subpart" (optional):
        Whether to combine mesh of subparts, default false.
    -- "neuron_list" (mandatory if choose "neuron_list" mode):
        The neuron names need to be combined.
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
            default_process_num=4):

        self.neuron_getter = NeuronRetriever(
            pymongoPath=pymongo_path,
            basePath=base_path,
            db_name=db_name,
            db_host=db_host,
            meshHierarchical_size=mesh_hierarchical_size,
            daisy_block_id_add_one_fix=daisy_block_id_add_one_fix,
            hierarchy_lut_path=hierarchy_lut_path,
            super_lut_pre=super_lut_pre
        )
        self.neuron_checker = NeuronChecker(
            db_dir=neuron_checker_dir,
            dahlia_db_name=dahlia_db_name,
            dahlia_db_host=dahlia_db_host
        )
        self.binary_mesh_path = binary_mesh_path
        self.default_proess_num = default_process_num

    def is_subpart(self, nid):
        result = bool(re.search('axon|dendrite|soma', nid))
        return result

    def trimesh_to_binary(self, trimesh_obj):
        triangles = np.array(trimesh_obj.faces).flatten()
        vertices = np.array(trimesh_obj.vertices)
        num_vertices = len(vertices)

        b_array = bytearray(4 + 4 * 3 * len(vertices) + 4 * len(triangles))
        # print(f'to_binary num_vertices: {num_vertices}')
        struct.pack_into('<I', b_array, 0, num_vertices)
        # print('Writing vertices ...')
        struct.pack_into('<' + 'f' * len(vertices.flatten()),
                            b_array,
                            4,
                            *vertices.flatten())
        # print('Writing faces ...')
        struct.pack_into('<' + 'I' * len(triangles),
                            b_array,
                            4 + 4 * 3 * num_vertices,
                            *triangles)
        return b_array

    def combine_mesh(self, nid, update=True, commit=False):
        with_child = self.is_subpart(nid)
        mesh_list, seg_set = self.neuron_getter.retrieve_neuron(nid, with_child=with_child)
        combined_trimesh = trimesh.util.concatenate(mesh_list)
        b_file = self.trimesh_to_binary(combined_trimesh)
        os.makedirs(self.binary_mesh_path, exist_ok=True)
        with open(os.path.join(self.binary_mesh_path, nid), 'wb') as f:
            f.write(b_file)
        if update:
            segment = json.dumps(list(seg_set))
            self.neuron_checker.update_neuron(nid=nid, tested=True, segments=segment, commit=commit)
        return seg_set

        
    def combine_mesh_list(self, nid_list, process_num=-1):

        def helper(nid_list, update=True, commit=False):
            for n in nid_list:
                self.combine_mesh(n, update=update, commit=commit)
                self.neuron_checker.commit_to_db()

        process_num = self.default_proess_num if process_num < 0 else process_num
        if process_num > 1:
            try:
                n_list = Manager().list()
                jobs = []
                neuron_lists = [[] for i in range(process_num)]
                for i, n in enumerate(nid_list):
                    neuron_lists[i % process_num].append(n)
                for neurons in neuron_lists:
                    jobs.append(Process(target=helper, args=(neurons, )))
                for j in jobs: j.start()
                for j in jobs: j.join()
            except Exception as e:
                print(e)
                print("switch to single thread")
                self.helper(nid_list)
        else:
            self.helper(nid_list)
    
    def combine_mesh_if_different(self, nid, commit=False):
        
        def helper(n, commit=False):
            mesh_list, seg_set_mongo = self.neuron_getter.retrieve_neuron(nid, with_child=True)
            seg_set_nc = set(json.loads(self.neuron_checker.get_neuron(nid)[1]))
            if seg_set_nc != seg_set_mongo:
                print(f'Difference detected, updating {nid} ...')
                self.combine_mesh(nid, commit=commit)
        
        try:
            iter(nid)
            for n in nid: helper(n, commit=commit)
            self.neuron_checker.commit_to_db()
        except TypeError as e:
            helper(n)
        
    def update_whole_neuron_version(self, include_subpart=True, process_num=-1):
        print('Checking new neurons')
        all_neuron_mongo = self.neuron_checker.all_neuron_mongo()
        all_neuron_nc = self.neuron_checker.get_all_neuron_name()
        new_neurons = set(all_neuron_mongo) - set(all_neuron_nc)
        print(f'new neurons: {new_neurons}')
        if not include_subpart:
            new_neurons = filter(lambda x: not self.is_subpart(x), new_neurons)
        for n in new_neurons:
            sql = '''INSERT INTO neuron (
                name, tested, subpart, segments)
                VALUES (?, ?, ?, ?)''' 
            subpart = 1 if self.is_subpart(n) else 0
            self.neuron_checker.update_query(sql, (n, 0, subpart, None))
        
        print('Checking untested neurons')
        subpart = None if include_subpart else False
        untested_neurons = self.neuron_checker.get_untested_neurons(subpart=subpart)
        self.combine_mesh_list(untested_neurons, process_num=process_num)

        print('Checking for difference')
        all_neuron = self.neuron_checker.get_all_neuron(subpart=subpart)
        process_num = self.default_proess_num if process_num < 0 else process_num
        if process_num > 1:
            try:
                n_list = Manager().list()
                jobs = []
                neuron_lists = [[] for i in range(process_num)]
                for i, n in enumerate(all_neuron):
                    neuron_lists[i % process_num].append(n)
                for neurons in neuron_lists:
                    jobs.append(Process(target=self.combine_mesh_if_different, args=(neurons, )))
                for j in jobs: j.start()
                for j in jobs: j.join()
            except Exception as e:
                print(e)
                print("switch to single thread")
                self.combine_mesh_if_different(all_neuron)
        else:
            self.combine_mesh_if_different(all_neuron)
        self.neuron_checker.commit_to_db()
            

#############################################################################


def test_combine_single_mesh(mc=None):
    if mc is None:
        mc = MeshCombiner()
    nid = 'grc_100'
    mc.combine_mesh(nid, commit=True)


def test_combine_mesh_list(mc=None, nlist=None):
    if mc is None: mc = MeshCombiner()
    if nlist is None: 
        nlist=['grc_100', 'grc_101', 'grc_102', 'interneuron_100.axon_0']
    mc.combine_mesh_list(nlist, process_num=2)


def test_diff(mc=None):
    if mc is None: mc = MeshCombiner()
    nid = 'grc_100'
    bad_segments = json.dumps(['123'])
    mc.neuron_checker.update_neuron('grc_100', 1, bad_segments)
    print(mc.neuron_checker.get_neuron(nid))
    mc.combine_mesh_if_different(nid)
    print(mc.neuron_checker.get_neuron(nid))


def test_whole_neuron_check(mc=None):
    if mc is None: mc = MeshCombiner()
    mc.update_whole_neuron_version(include_subpart=False)


def main():
    modes = {
        'autocheck': {'autocheck', 'ac', 'check'},
        'neuron_list': {'neuron_list', 'nl', 'list'}
    }

    if len(sys.argv) != 2:
        print("Wrong parameter format!")
        print(MANUAL)
        exit(1)

    file_path = sys.argv[1]
    try:
        with open(file_path, 'r') as f:
            config_file = json.load(f)
        database_config = config_file['database_config']
        mode_config = config_file['mode_config']
    except Exception as e:
        print(e)
        print(MANUAL)
        exit(1)
    
    mc = MeshCombiner(*database_config)
    mode = mode_config['mode']
    
    process_num = mode_config['process_num'] if 'process_num' in mode_config else -1
    include_subpart = mode_config['include_subpart'] if 'include_subpart' in mode_config else False
    
    if mode in mode['autocheck']:
        mc.update_whole_neuron_version(
            include_subpart=include_subpart,
            process_num=process_num)
    elif mode in mode['neuron_list']:
        nlist = config_file['mode_config']['neuron_list']
        mc.combine_mesh_list(
            nlist,
            process_num=process_num)


if __name__ == "__main__":
    # main()
    # test_combine_single_mesh()
    # test_combine_mesh_list()
    # test_diff()
    test_whole_neuron_check()
