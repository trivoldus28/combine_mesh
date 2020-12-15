# Combine mesh

usage:

``` sh
activatesk # or activatedaisy
python combine_mesh.py config.json
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
    "database_config": {
      "pymongo_path": "/n/groups/htem/Segmentation/tmn7/segwaytool.proofreading/segwaytool/proofreading/",
      "base_path": "/n/balin_tank_ssd1/htem/Segmentation/cb2_v4/output.zarr/meshes/precomputed/mesh/",
      "db_name": "neurondb_cb2_v4",
      "db_host": "mongodb://10.117.28.250:27018/",
      "mesh_hierarchical_size": 10000,
      "daisy_block_id_add_one_fix": true,
      "hierarchy_lut_path": "/n/balin_tank_ssd1/htem/Segmentation/cb2_v4/output.zarr/luts/fragment_segment",
      "super_lut_pre": "super_1x2x2_hist_quant_50",
      "neuron_checker_dir": "/n/groups/htem/Segmentation/xg76/combine_mesh/neuron_check/neuron_mesh.db",
      "dahlia_db_name": "neurondb_cb2_v4",
      "dahlia_db_host": "mongodb://10.117.28.250:27018/",
      "default_process_num": 4
    },
  
    "mode_config":{
      "mode": "autocheck",
      "process_num": 4,
      "include_subpart": true
    }
  }
```

For `neuron_list` mode, change mode_config to something like:
```json
{
    "mode": "neuron_list",
    "neuron_list": ["grc_100", "interneuron_100"],
    "process_num": 2,
    "include_subpart": true
}
```
