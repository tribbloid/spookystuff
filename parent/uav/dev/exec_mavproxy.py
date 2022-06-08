import pkgutil

import sys
from MAVProxy import mavproxy

pkgutil.find_loader(mavproxy.__name__)

loader = pkgutil.find_loader(mavproxy.__name__)
fileName = loader.get_filename(mavproxy.__name__)

print("main script: ", fileName)

oldArgs = sys.argv[1:]
# customize launching option here

exec(open(fileName).read())
