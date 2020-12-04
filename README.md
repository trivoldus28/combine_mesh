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

To enable, just set the mod to 'neuron_list'

Again, don't go to very big process num.
