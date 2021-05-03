import json
import glob
from get_indicateurs import *
import multiprocessing as mp

res = {}


res['taux_incidence'] = getTauxIncidence()
res['hospitalisations'] = getHospitalisations()
res['hospitalisations_moyenne_quotidien'] = getMeanHospitalisations()
res['soins_critiques'] = getReas()
res['soins_critiques_moyenne_quotidienne'] = getMeanReas()
res['deces'] = getDeces()
res['deces_moyenne_quotidien'] = getMeanDeces()
res['retour_a_domicile'] = getRad()
res['retour_a_domicile_moyenne_quotidienne'] = getMeanRad()
res['vaccins_premiere_dose'] = getFirstDoseVaccins()
res['vaccins_premiere_dose_moyenne_quotidienne'] = getMeanFirstDoseVaccins()
res['vaccins_vaccines'] = getFullVaccins()
res['cas_positifs'] = getCasPositifs()
res['taux_positivite'] = getTauxPositivite()

for item in res:
    with open('dist/'+item+'.json','w') as fp:
        json.dump(res[item], fp)

resglobal = {}
files = glob.glob("dist/*.json")

for file in files:
    with open(file) as json_file:
        print(file.replace('dist/','').replace('.json',''))
        resglobal[file.replace('dist/','').replace('.json','')] = json.load(json_file)

with open('global_new.json','w') as fp:
    json.dump(resglobal, fp)
 
