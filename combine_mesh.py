from neuron_getter import NeuronRetriever
import trimesh
from neuron_check_mesh import NeuronChecker
from tqdm import tqdm
import struct
import sys
import numpy as np
import os
import re
import json
from multiprocessing import Process, Manager
import logging
import datetime


MANUAL = """
Welcome to combine mesh.

Please enter the [config_file_path]

The config file should be a json file containing databse configs and 
mode configs. The json file should have three objects: "database_config",
"output_path" and "mode_config". database_config might just be empty 
since all of the fields have default values.

Possible fields in database_config:
    -- pymongo_path
    -- base_path
    -- db_name
    -- db_host
    -- mesh_hierarchical_size
    -- daisy_block_id_add_one_fix
    -- neuron_checker_dir
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
            neuron_checker_dir='/n/groups/htem/Segmentation/xg76/combine_mesh/neuron_check/neuron_mesh.db',
            binary_mesh_path='/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh',
            default_process_num=4):

        self.neuron_getter = NeuronRetriever(
            pymongoPath=pymongo_path,
            basePath=base_path,
            db_name=db_name,
            db_host=db_host,
            meshHierarchical_size=mesh_hierarchical_size,
            daisy_block_id_add_one_fix=daisy_block_id_add_one_fix
        )
        self.neuron_checker = NeuronChecker(
            db_dir=neuron_checker_dir
        )
        self.binary_mesh_path = binary_mesh_path
        self.default_proess_num = default_process_num

    # check the name of a neuron to determin whether it is a subpart
    def is_subpart(self, nid):
        result = bool(re.search('axon|dendrite|soma|unknown_segment', nid))
        return result

    # convert trimesh to binary file.
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

    # combine a neuron given neuron id.
    def combine_mesh(self, nid, update=True, commit=False):
        logging.info(f'Combining {nid}...')
        with_child = self.is_subpart(nid)
        try:
            mesh_list, seg_set = self.neuron_getter.retrieve_neuron(
                nid, with_child=with_child)
        except Exception as e:
            logging.info(e)
            logging.info(f'Failed to retrieve mesh: {nid}')
            return ""
        combined_trimesh = trimesh.util.concatenate(mesh_list)
        b_file = self.trimesh_to_binary(combined_trimesh)
        os.makedirs(self.binary_mesh_path, exist_ok=True)
        with open(os.path.join(self.binary_mesh_path, nid), 'wb') as f:
            f.write(b_file)
        if update:
            segment = json.dumps(list(seg_set))
            self.neuron_checker.update_neuron(
                nid=nid, tested=True, segments=segment, commit=commit)
        return seg_set

    # combine mesh from a list of neuron ids
    def combine_mesh_list(self, nid_list, process_num=-1, include_subpart=False):

        # get sub part of a neuron list
        # [interneuron_1, grc_30] -> [interneuroon_1.axon_0, grc_30.axon_0 ....]
        def get_subpart(n_list):
            subparts = []
            for n in n_list:
                if not self.is_subpart(n):
                    subparts.extend(self.neuron_getter.get_children(n))
            return subparts

        # helper to combine list of nids
        def helper(n_list, update=True, commit=True):
            p_bar = tqdm(total=len(n_list), desc='Combining Mesh List')
            for n in n_list:
                self.combine_mesh(n, update=update, commit=commit)
                p_bar.update(1)
            p_bar.close()
            self.neuron_checker.commit_to_db()

        # process num: for multiprocessing
        # -1: default process num, 0 or 1: single process, 1+: multi process
        process_num = self.default_proess_num if process_num < 0 else process_num
        if include_subpart:
            nid_list.extend(get_subpart(nid_list))
        logging.info(f'Full neuron list for combine: {nid_list}')
        if process_num > 1:
            try:
                n_list = Manager().list()
                jobs = []
                neuron_lists = [[] for i in range(process_num)]
                for i, n in enumerate(nid_list):
                    neuron_lists[i % process_num].append(n)
                for neurons in neuron_lists:
                    jobs.append(Process(target=helper, args=(neurons, )))
                for j in jobs:
                    j.start()
                for j in jobs:
                    j.join()
            except Exception as e:
                logging.debug(e)
                logging.info("switch to single thread")
                helper(nid_list)
        else:
            helper(nid_list, commit=False)

    # check whether the old mesh is changed. If changed, recombine
    def combine_mesh_if_different(self, nid, commit=True):

        # helper function to combine given neuron id
        def combine_if_different(n, commit=True):
            try:
                mesh_list, seg_set_mongo = self.neuron_getter.retrieve_neuron(
                    n, with_child=True)
            except Exception as e:
                logging.info(f'retrieve {n} failed, skipping ...')
                return
            seg_set_nc = set(json.loads(self.neuron_checker.get_neuron(n)[1]))
            if seg_set_nc != seg_set_mongo:
                logging.info(f'Difference detected, updating {n} ...')
                self.combine_mesh(n, commit=commit)

        if isinstance(nid, list):
            p_bar = tqdm(total=len(nid), desc='Checking Difference')
            for n in nid:
                combine_if_different(n, commit=commit)
                p_bar.update(1)
            p_bar.close()
            self.neuron_checker.commit_to_db()
        else:
            combine_if_different(n, commit=commit)

    # 1. check not existing neurons
    # 2. combine un-combined neurons
    # 3. check difference, if is different, recombine
    def update_whole_neuron_version(self, include_subpart=True, process_num=-1):
        logging.info('Checking new neurons')
        all_neuron_mongo = self.neuron_getter.get_all_neuron_name()
        all_neuron_nc = self.neuron_checker.get_all_neuron_name()
        new_neurons = set(all_neuron_mongo) - set(all_neuron_nc)

        if not include_subpart:
            new_neurons = list(
                filter(lambda x: not self.is_subpart(x), new_neurons))
        if len(new_neurons) > 0:
            logging.info(f'new neurons found: {new_neurons}')
        else:
            logging.info('No new neurons found')
        for n in new_neurons:
            sql = '''INSERT INTO neuron (
                name, tested, subpart, segments)
                VALUES (?, ?, ?, ?)'''
            is_sp = 1 if self.is_subpart(n) else 0
            self.neuron_checker.update_query(sql, (n, 0, is_sp, None))

        logging.info('Checking untested neurons')
        subpart = None if include_subpart else False
        untested_neurons = self.neuron_checker.get_untested_neurons(
            subpart=subpart)
        if len(untested_neurons) > 0:
            logging.info(f'Untested neurons found: {untested_neurons}')
        else:
            logging.info('No untested neurons found')
        self.combine_mesh_list(
            untested_neurons,
            process_num=process_num)

        logging.info('Checking for difference')
        all_neuron = self.neuron_checker.get_all_neuron_name(subpart=subpart)
        process_num = self.default_proess_num if process_num < 0 else process_num
        if process_num > 1:
            try:
                n_list = Manager().list()
                jobs = []
                neuron_lists = [[] for i in range(process_num)]
                for i, n in enumerate(all_neuron):
                    neuron_lists[i % process_num].append(n)
                for neurons in neuron_lists:
                    jobs.append(
                        Process(target=self.combine_mesh_if_different, args=(neurons, True)))
                for j in jobs:
                    j.start()
                for j in jobs:
                    j.join()
            except Exception as e:
                logging.debug(e)
                logging.info("switch to single thread")
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
    if mc is None:
        mc = MeshCombiner()
    if nlist is None:
        nlist = ['grc_100', 'grc_101', 'grc_102', 'interneuron_100.axon_0']
    mc.combine_mesh_list(nlist, process_num=2)


def test_diff(mc=None):
    if mc is None:
        mc = MeshCombiner()
    nid = ['grc_100', 'grc_101', 'grc_102']
    bad_segments = json.dumps(['123'])
    mc.neuron_checker.update_neuron('grc_100', 1, bad_segments)
    for n in nid:
        print(mc.neuron_checker.get_neuron(n))
    mc.combine_mesh_if_different(nid)
    for n in nid:
        print(mc.neuron_checker.get_neuron(n))


def test_whole_neuron_check(mc=None):
    if mc is None:
        mc = MeshCombiner()
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
        output_path = config_file['output_path']
        database_config = config_file['database_config']
        mode_config = config_file['mode_config']
        database_config['binary_mesh_path'] = output_path
    except Exception as e:
        print(e)
        print("Wrong with config file, Display manual:")
        print(MANUAL)
        exit(1)

    now = datetime.datetime.now().strftime('%m%d.%H.%M.%S')

    # create log path
    if 'log_path' in config_file:
        log_path = config_file['log_path']
    else:
        log_path = os.path.join(output_path, 'log')
    os.makedirs(log_path, exist_ok=True)

    log_name = os.path.join(log_path, str(now)+'.log')
    logging.basicConfig(filename=log_name, filemode='w', level=logging.INFO)
    # logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logging.info(f"Your log file: {log_name}")
    logging.info(f"Your config file: {config_file}")

    try:
        mc = MeshCombiner(**database_config)
    except Exception as e:
        logging.debug(e)
        logging.debug(f'Your config: {database_config}')
        logging.info(
            'Init mesh combiner failed, something wrong with database_config')
        exit(1)

    try:
        mode = mode_config['mode']
    except Exception as e:
        logging.debug(e)
        logging.info('Fail to find mode')
        exit(1)

    process_num = mode_config['process_num'] if 'process_num' in mode_config else -1
    include_subpart = mode_config['include_subpart'] if 'include_subpart' in mode_config else False

    if mode in modes['autocheck']:
        logging.info('GO TO autocheck MODE')
        mc.update_whole_neuron_version(
            include_subpart=include_subpart,
            process_num=process_num)
    elif mode in modes['neuron_list']:
        logging.info('GO TO neuron_list MODE')
        nlist = mode_config['neuron_list']
        mc.combine_mesh_list(
            nlist,
            process_num=process_num,
            include_subpart=include_subpart)
    else:
        logging.info(f'Cannot understand mode: {mode}')
        exit(1)


if __name__ == "__main__":
    # mc = MeshCombiner(
    #     hierarchy_lut_path= "/n/balin_tank_ssd1/htem/Segmentation/cb2_v4/output.zarr/luts/fragment_segment",
    #     base_path="/n/balin_tank_ssd1/htem/Segmentation/cb2_v4/output.zarr/meshes/precomputed/mesh/"
    # )
    # test_diff(mc)
    # test_combine_single_mesh(mc)
    # test_combine_mesh_list(mc)
    # test_whole_neuron_check(mc)
    main()
