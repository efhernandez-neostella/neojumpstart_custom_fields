import os
import sys

# note: this script allow to support relative modules importing
path_to_add = os.path.dirname(os.path.realpath(__file__))

for index in range(1, 3):
    path_to_add = os.path.dirname(path_to_add)

sys.path.append(path_to_add)
