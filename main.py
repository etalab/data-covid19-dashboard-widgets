import json
from get_indicateurs import getTauxIncidence, getHospitalisations, getReas, getVaccins, getCasPositifs

resGlobal = {}

#resGlobal['tx_incidence'] = getTauxIncidence()
#resGlobal['hospitalisations'] = getHospitalisations()
#resGlobal['reas'] = getReas()
resGlobal['premieres_injections'] = getVaccins()
resGlobal['cas_positifs'] = getCasPositifs()

with open('result.json','w') as fp:
    json.dump(resGlobal, fp)