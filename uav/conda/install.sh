
CRDIR="$(cd "`dirname "$0"`"; pwd)"

conda env create --force -f ${CRDIR}/conda-env.yml --prefix ${CRDIR}/.env
