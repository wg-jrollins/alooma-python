"""
This package includes all the submodules
to be loaded by the Alooma Python API
"""
import os

# This code registers and imports all the submodules
# in the submodules package
SUBMODULES = []
for module in os.listdir(os.path.dirname(__file__)):
    ext = module[-3:]
    module_name = module[:-3]
    if module == '__init__.py' or ext != '.py':
        continue
    SUBMODULES.append(module_name)
    __import__(module_name, locals(), globals())
del module
