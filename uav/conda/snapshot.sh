
CRDIR="$(cd "`dirname "$0"`"; pwd)"

conda env export --no-builds > ${CRDIR}/conda-env.yml

