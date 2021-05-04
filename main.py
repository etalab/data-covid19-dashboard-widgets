import json
import glob
from utils import *
from download_and_check_files import *

kpis = download_and_check()

#kpis = ['hospitalisations', 'hospitalisations_moyenne_quotidienne', 'retour_a_domicile', 'retour_a_domicile_moyenne_quotidienne', 'soins_critiques', 'soins_critiques_moyenne_quotidienne', 'deces', 'deces_moyenne_quotidienne', 'cas_positifs', 'taux_incidence', 'taux_positivite', 'vaccins_premiere_dose', 'vaccins_premiere_dose_moyenne_quotidienne', 'vaccins_vaccines']

print(kpis)

res = {}

if('taux_incidence' in kpis):
    res['taux_incidence'] = getTxGeneric('taux_incidence')
    
if('taux_positivite' in kpis):
    res['taux_positivite'] = getTxGeneric('taux_positivite')
    
if('cas_positifs' in kpis):
    res['cas_positifs'] = getKPIGeneric('cas_positifs','pos_7j',False)
    
if('hospitalisations' in kpis):
    res['hospitalisations'] = getKPIGeneric('hospitalisations','hosp',False)
    
if('hospitalisations_moyenne_quotidienne' in kpis):
    res['hospitalisations_moyenne_quotidienne'] = getKPIGeneric('hospitalisations_moyenne_quotidienne','incid_hosp',True)
    
if('soins_critiques' in kpis):
    res['soins_critiques'] = getKPIGeneric('soins_critiques','rea',False)
    
if('soins_critiques_moyenne_quotidienne' in kpis):
    res['soins_critiques_moyenne_quotidienne'] = getKPIGeneric('soins_critiques_moyenne_quotidienne','incid_rea',True)
    
if('deces' in kpis):
    res['deces'] = getKPIGeneric('deces','dchosp',False) 
    
if('deces_moyenne_quotidienne' in kpis):
    res['deces_moyenne_quotidienne'] = getKPIGeneric('deces_moyenne_quotidienne','incid_dchosp',True)
    
if('retour_a_domicile' in kpis):
    res['retour_a_domicile'] = getKPIGeneric('retour_a_domicile','rad',False)
    
if('retour_a_domicile_moyenne_quotidienne' in kpis):
    res['retour_a_domicile_moyenne_quotidienne'] = getKPIGeneric('retour_a_domicile_moyenne_quotidienne','incid_rad',True)
    
if('vaccins_premiere_dose' in kpis):
    res['vaccins_premiere_dose'] = getKPIGeneric('vaccins_premiere_dose','n_cum_dose1',False,True)
    
if('vaccins_premiere_dose_moyenne_quotidienne' in kpis):
    res['vaccins_premiere_dose_moyenne_quotidienne'] = getKPIGeneric('vaccins_premiere_dose_moyenne_quotidienne','n_dose1',True,True)
    
if('vaccins_vaccines' in kpis):
    res['vaccins_vaccines'] = getKPIGeneric('vaccins_vaccines','n_cum_complet',False,True)
 
saveNewFiles()