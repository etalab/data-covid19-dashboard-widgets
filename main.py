import json
import glob
from get_indicateurs import getTauxIncidence, getHospitalisations, getReas, getVaccins, getCasPositifs, getDeces

res = {}
""" 
try:
    res['taux_incidence'] = getTauxIncidence()
except:
    print('ERROR - Taux incidence')

try:
    res['hospitalisations'] = getHospitalisations()
except:
    print('ERROR - Hospitalisations')

try:
    res['soins_critiques'] = getReas()
except:
    print('ERROR - Réanimations')

try:
    res['deces'] = getDeces()
except:
    print('ERROR - Décès')

try:
    res['vaccins'] = getVaccins()
except:
    print('ERROR - Vaccins')

try:
    res['cas_positifs'] = getCasPositifs()
except:
    print('ERROR - Cas Positifs')

for item in res:
    with open('data/'+item+'.json','w') as fp:
        json.dump(res[item], fp)
 """
resglobal = {}
files = glob.glob("data/*.json")
for file in files:
    with open(file) as json_file:
        resglobal[file.replace('data/','').replace('.json','')] = json.load(json_file)

with open('global.json','w') as fp:
    json.dump(resglobal, fp)

