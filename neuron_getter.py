from cachetools import cached, RRCache
import pymongo
# this is the package that does the computations with the meshes; it requires that Rtree be installed (pip install rtree)
import trimesh
import os
import struct
import numpy as np
import sys
try: 
    from dahlia.db_server import NeuronDBServer
    from dahlia.connected_segment_server import ConnectedSegmentServer
except ModuleNotFoundError as e:
    print(e)
    print("Using segway.dahlia failed")
    print("try importing dahalia")
    from segway.dahlia.db_server import NeuronDBServer
    from segway.dahlia.connected_segment_server import ConnectedSegmentServer

try:
    import matplotlib.pyplot as plt
except:
    print('matplotlib unavailable')

try:
    import daisy
    from PIL import Image, ImageDraw
except:
    print('daisy unavailable')

pymongoPath = '/n/groups/htem/Segmentation/tmn7/segwaytool.proofreading/segwaytool/proofreading/'
sys.path.insert(0, pymongoPath)


class Touch:

    def __init__(
            self,
            pymongoPath='/n/groups/htem/Segmentation/tmn7/segwaytool.proofreading/segwaytool/proofreading/',
            basePath='/n/balin_tank_ssd1/htem/Segmentation/cb2_v4/output.zarr/meshes/precomputed/mesh/',
            db_name='neurondb_cb2_v4',
            db_host='mongodb://10.117.28.250:27018/',
            meshHierarchical_size=10000,
            daisy_block_id_add_one_fix=True,
    ):

        if daisy_block_id_add_one_fix:
            daisy.block.Block.BLOCK_ID_ADD_ONE_FIX = True

        self.pymongoPath = pymongoPath
        self.basePath = basePath
        assert(os.path.exists(self.basePath))
        self.db_name = db_name
        self.db_host = db_host
        self.meshHierarchical_size = meshHierarchical_size

        self.neuron_db = self.get_neuron_db()
        self.connect_db = self.get_connect_db()


    def close_connection(self):
        self.neuron_db.close()


    def get_connect_db(self):
        hierarchy_lut_path = '/n/f810/htem/Segmentation/cb2_v4/output.zarr/luts/fragment_segment'
        super_lut_pre = 'super_1x2x2_hist_quant_50'
        connect_server = ConnectedSegmentServer(
            hierarchy_lut_path=hierarchy_lut_path,
            super_lut_pre=super_lut_pre,
            voxel_size=(40, 8, 8),
            find_segment_block_size=(4000, 4096, 4096),
            super_block_size=(4000, 8192, 8192),
            fragments_block_size=(400, 2048, 2048),
            super_offset_hack=(2800, 0, 0),
            base_threshold=0.5)
        return connect_server

    def get_neuron_db(self):
        try:
            ndb = NeuronDBServer(db_name=self.db_name, host=self.db_host)
            return ndb 
        except Exception as e:
            raise ConnectionError('Failed to connect to neuron db server.')

    def getHierarchicalMeshPath(self, object_id):
        # finds the path to mesh files based on segment numbers
        assert object_id != 0
        level_dirs = []
        num_level = 0
        while object_id > 0:
            level_dirs.append(int(object_id % self.meshHierarchical_size))
            object_id = int(object_id / self.meshHierarchical_size)
        num_level = len(level_dirs) - 1
        level_dirs = [str(lv) for lv in reversed(level_dirs)]
        # print(os.path.join(str(num_level), *level_dirs))
        return os.path.join(str(num_level), *level_dirs)

    def getNeuronSubsegments(self, nid, segment_name):
        # collects segment numbers for all meshes designated as nid.segmentName_# for any int(#)
        num = 0
        out_segments = list()
        while True:
            try:
                new_segments = self.neuron_db.get_neuron(
                    nid+'.'+segment_name+'_'+ str(num)).to_json()['segments']
                num += 1
                try:
                    out_segments.extend(new_segments)
                except:
                    out_segments = new_segments
            except:
                break
        return out_segments

    @cached(cache=RRCache(maxsize=1024*1024))
    def getMesh(self, segmentNum):
        '''opens mesh file from local directory and parses it
        returning a trimesh object.
        Returns `None` if segment does not exist
        '''
        base = self.basePath
        workfile = base + self.getHierarchicalMeshPath(int(segmentNum))
        # print(workfile); exit()

        try:
            totalSize = os.stat(workfile).st_size
            with open(workfile, 'rb') as f:
                num_vertices = struct.unpack('<I', memoryview(f.read(4)))[-1]
                vertices = np.empty((num_vertices, 3))
                for i in range(num_vertices):
                    vertices[i, ] = struct.unpack('<fff', memoryview(f.read(12)))
                num_triangles = int((totalSize - (num_vertices*12 + 4))/12)
                triangles = np.empty((num_triangles, 3))
                for i in range(num_triangles):
                    triangles[i, ] = struct.unpack('<III', memoryview(f.read(12)))
            mesh = trimesh.Trimesh(vertices=vertices, faces=triangles)
            # this line @cached(cache=RRCache(maxsize=128*1024*1024))
            # makes Python caches the last 1024*1024 recently used meshes
            # if a mesh is 100KB, then that would be ~100GB of memory
            return mesh
        except:
            return None

    def getNeuronSegId(self, nid):
        segmentNums = self.neuron_db.get_neuron(nid).to_json()['segments']
        dendrite_segments = self.getNeuronSubsegments(nid, 'dendrite')
        soma_segments = self.getNeuronSubsegments(nid, 'soma')
        axon_segments = self.getNeuronSubsegments(nid, 'axon')
        all_segments = set(segmentNums + dendrite_segments +
                           soma_segments + axon_segments)
        return all_segments

    def getAllMesh(self, nid):
        """Return all the segments of a given neuron

        Args:
            nid (str): the neuron name.

        Returns:
            : all segments of a neuron in trimesh
        """
        all_segments = self.getNeuronSegId(nid)
        return self.getMeshes(all_segments)


    def getMeshes(self, seg_num_list):
        """Get all the meshes given segment id list

        Args:
            seg_num_list ([int]): segment id list

        Returns:
            [Trimesh]: list of the trimesh of segments
        """
        meshes = []
        iT, iF = 0, 0
        for num in seg_num_list:
            m = self.getMesh(int(num))
            if m:
                iT += 1
                meshes.append(m)
            else:
                iF += 1
                # print('Missing mesh # {} || {} failed of {} ({:.3f}%)'.format(
                #     int(num), iF, iT, 100 if iT == 0 else 100*iF/iT))
        assert iT
        return meshes


class NeuronRetriever:

    def __init__(self):
        self.t = Touch()
        self.db = self.t.get_neuron_db()

    def retrieve_soma_loc(self, neuron):
        loc = self.db.get_neuron(neuron).to_json()
        try:
            n_loc = [
                int(loc['soma_loc']['x']) / 4,
                int(loc['soma_loc']['y']) / 4,
                int(loc['soma_loc']['z'])
            ]
            return n_loc
        except:
            return None

    def retrieve_neuron(self, neuron, store=True, path=None, soma_loc=False):
        neuron_mesh = self.t.getAllMesh(neuron)
        n_loc = None
        if soma_loc:
            n_loc = self.retrieve_soma_loc(neuron)
        neuron_dict = {'neuron_name': neuron,
                       'soma_loc': n_loc}
        if store:
            neuron_dict['meshes'] = self.mesh_to_csv(neuron_mesh)
            self.store_to_path(neuron_dict, neuron, path)
        
        neuron_dict['meshes'] = neuron_mesh
        return neuron_dict

    def store_to_path(self, obj, obj_name, path, format='pickle'):
        if path is None:
            print('No path is finded! Will not store to file.')
        else:
            if format == 'pickle' or format == 'p':
                with open(os.path.join(path, obj_name), 'wb') as f:
                    pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
            elif format == 'json' or format == 'j':
                with open(os.path.join(path, obj_name), 'w') as f:
                    json.dump(obj, f)

    def mesh_to_csv(self, mesh_list):
        mesh_csv_list = []
        pbar = tqdm(total=len(mesh_dict), desc='Transfer Mesh to JSON')
        for mesh in mesh_list():
            mesh_v_df = pd.DataFrame(mesh.vertices)
            mesh_f_df = pd.DataFrame(mesh.faces)
            mesh_csv_list.append({
                'v': mesh_v_df.to_csv(index=False, header=False),
                'f': mesh_f_df.to_csv(index=False, header=False)
            })
            pbar.update(1)
        pbar.close()
        return mesh_csv_list