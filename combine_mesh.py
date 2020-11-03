from neuron_getter import NeuronRetriever
import trimesh
from neuron_check_mesh import NeuronChecker
import pandas as pd 
from tqdm import tqdm
import sys

NEURON_GETTER = NeuronRetriever()
NEURON_CHECKER = NeuronChecker()
WRITE_RATE = 20

def combine_mesh(neuron_class, nid, commit=False):
    mesh_dict = NEURON_GETTER.retrieve_neuron(nid, store=False, path=None, soma_loc=False)
    mesh_list = mesh_dict['meshes']
    combined_trimesh = trimesh.util.concatenate(mesh_list)
    content = combined_trimesh.to_dict()

    faces = pd.DataFrame(content['faces']).to_csv(index=False, header=False)
    face_normals = pd.DataFrame(content['face_normals']).to_csv(index=False, header=False)
    vertices = pd.DataFrame(content['vertices']).to_csv(index=False, header=False)

    NEURON_CHECKER.update_neuron(
        neuron_class, nid, True, 
        faces=faces, face_normals=face_normals, vertices=vertices,
        commit=commit)


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
    main()