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


class NeuronRetriever:

    def __init__(
            self,
            pymongoPath='/n/groups/htem/Segmentation/tmn7/segwaytool.proofreading/segwaytool/proofreading/',
            basePath='/n/f810/htem/Segmentation/cb2_v4/output.zarr/meshes/precomputed_v2/mesh/',
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

    def getMesh(self, segmentNum, raw=False):
        '''opens mesh file from local directory and parses it
        returning a trimesh object.
        Returns `None` if segment does not exist
        '''
        base = self.basePath
        workfile = base + self.getHierarchicalMeshPath(int(segmentNum))

        try:
            totalSize = os.stat(workfile).st_size
            with open(workfile, 'rb') as f:
                num_vertices = struct.unpack('<I', memoryview(f.read(4)))[-1]
                # print(f'get mesh num_vertices: {num_vertices}')
                vertices = np.empty((num_vertices, 3))
                for i in range(num_vertices):
                    vertices[i, ] = struct.unpack('<fff', memoryview(f.read(12)))
                num_triangles = int((totalSize - (num_vertices*12 + 4))/12)
                triangles = np.empty((num_triangles, 3))
                for i in range(num_triangles):
                    triangles[i, ] = struct.unpack('<III', memoryview(f.read(12)))
            if raw:
                return (vertices, triangles)
            else:
                mesh = trimesh.Trimesh(vertices=vertices, faces=triangles)
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

    def getMeshes(self, seg_num_list, raw=False):
        meshes = []
        iT, iF = 0, 0
        for num in seg_num_list:
            m = self.getMesh(int(num), raw=raw)
            if m:
                iT += 1
                meshes.append(m)
            else:
                iF += 1
        assert iT
        return meshes

    def retrieve_neuron(self, nid, raw=False):
        all_segments = self.getNeuronSegId(nid)
        return self.getMeshes(all_segments, raw=raw)
