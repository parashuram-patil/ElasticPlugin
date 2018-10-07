Creation of plugin:

- Run gradle plugin here to create the plugin zip
- Zip gets created in the build/distributions folder
- Copy this path
- navigate to the bin folder of elasticsearch
- run command : plugin install file:/{path to zip}
- if plugin already exists, first remove it using plugin remove 'elasticsearch-plugin'
