# Combine mesh

usage:

``` sh
activatedaisy 
# Add following lines to .bashrc if failed:
# alias activatedaisy='source /n/groups/htem/users/tmn7/envs/ubuntu180402/bin/activate'
# export PYTHONPATH=$PYTHONPATH:/usr/lib/python3/dist-packages/graph_tool_hack
python combine_mesh.py [path to config]
```
###

if there is no config file provided, it won't run and will display the manual page.

### Autocheck:

This mode does:

1. check whether there is some new enurons not in the database

2. combine the neurons in the database shows no record of mesh combining

3. Check all neurons in the database to see if it need update

To enable, just set the mode to 'autocheck' or 'ac'

Don't go to very big process num since writing to check db is not fast.

'include_subpart' gives an option to also do an whole update on subparts.

### neuron list:

This mode does:

1. combine mesh in the given list

2. update the mesh pieces in the database

To enable, just set the mode to 'neuron_list'

Again, don't go to very big process num.


### sample config:

```json
{ 
    "ouput_path": "/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh_test",
    "log_path": "/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh_test/log",
    "db_dir": "/n/groups/htem/Segmentation/xg76/combine_mesh/binary_mesh_test/db/neuron_mesh.db",
    "database_config": {
      "pymongo_path": "/n/groups/htem/Segmentation/tmn7/segwaytool.proofreading/segwaytool/proofreading/",
      "base_path": "/n/balin_tank_ssd1/htem/Segmentation/cb2_v4/output.zarr/meshes/precomputed/mesh/",
      "db_name": "neurondb_cb2_v4",
      "db_host": "mongodb://10.117.28.250:27018/",
      "mesh_hierarchical_size": 10000,
      "daisy_block_id_add_one_fix": true
    },
  
    "mode_config":{
      "mode": "autocheck",
      "process_num": 4,
      "include_subpart": true
    }
  }
```
The `log_path` and `db_dir` are optional. If they are not specified, they are going to be `{output_path}/log`, `{output_path}/db/neuron_mesh.db` respectively.

For `neuron_list` mode, change mode_config to something like:
```json
{
    "mode": "neuron_list",
    "neuron_list": ["grc_100", "interneuron_100"],
    "process_num": 2,
    "include_subpart": true,
    "overwrite": false
}
```
If overwrite is true, it will combine the mesh without checking with neuron_checker (but will update the db).
If overwrite is false, it only mesh when segments are different.

### First time user: Init sqlite neuron_mesh.db

** You are not required to init your own db in this version**
You can enter your desired db path in config file under "db_dir". Or just skip this step and the script will init the db under '{output_path}/db'

If you are not using exisiting neuron_mesh.db in `/n/groups/htem/Segmentation/xg76/combine_mesh/neuron_check/neuron_mesh.db`, you can init your own db:
```shell
activatedaisy
python init_neuron_checker.py [path to sqlite db with name] [neuron_db_name] [neuron_db_host]
# For example:
# python init_neuron_checker.py /n/groups/htem/Segmentation/xg76/combine_mesh/neuron_check/neuron_mesh_test.db neurondb_cb2_v4 mongodb://10.117.28.250:27018/
```
