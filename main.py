import json
import glob
from utils import *

res = {}

res['taux_incidence'] = getTxGeneric('taux_incidence')
res['taux_positivite'] = getTxGeneric('taux_positivite')
res['cas_positifs'] = getKPIGeneric('cas_positifs','pos_7j',False)
res['hospitalisations'] = getKPIGeneric('hospitalisations','hosp',False)
res['hospitalisations_moyenne_quotidienne'] = getKPIGeneric('hospitalisations_moyenne_quotidienne','incid_hosp',True)
res['soins_critiques'] = getKPIGeneric('soins_critiques','rea',False)
res['soins_critiques_moyenne_quotidienne'] = getKPIGeneric('soins_critiques_moyenne_quotidienne','incid_rea',True)
res['deces'] = getKPIGeneric('deces','dchosp',False) 

res['deces_moyenne_quotidienne'] = getKPIGeneric('deces_moyenne_quotidienne','incid_dchosp',True)
res['retour_a_domicile'] = getKPIGeneric('retour_a_domicile','rad',False)
res['retour_a_domicile_moyenne_quotidienne'] = getKPIGeneric('retour_a_domicile_moyenne_quotidienne','incid_rad',True)
res['vaccins_premiere_dose'] = getKPIGeneric('vaccins_premiere_dose','n_cum_dose1',False,True)
res['vaccins_premiere_dose_moyenne_quotidienne'] = getKPIGeneric('vaccins_premiere_dose_moyenne_quotidienne','n_dose1',True,True)
res['vaccins_vaccines'] = getKPIGeneric('vaccins_vaccines','n_cum_complet',False,True)
 
resglobal = {}
files = glob.glob("dist/*.json")

for file in files:
    with open(file) as json_file:
        print(file.replace('dist/','').replace('.json',''))
        resglobal[file.replace('dist/','').replace('.json','')] = json.load(json_file)

with open('global_new.json','w') as fp:
    json.dump(resglobal, fp)
 
