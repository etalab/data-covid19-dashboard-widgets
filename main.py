import json
import glob
from utils import get_taux, get_kpi, get_kpi_only_france, get_taux_specific
from download_and_check_files import download_and_check, save_new_files, shorten_and_save
from logger import log

# kpis = download_and_check()

# kpis = ['hospitalisations', 'hospitalisations_moyenne_quotidienne', 'retour_a_domicile', 'retour_a_domicile_moyenne_quotidienne', 'soins_critiques', 'soins_critiques_moyenne_quotidienne', 'deces', 'deces_moyenne_quotidienne', 'cas_positifs', 'taux_incidence', 'taux_positivite', 'vaccins_premiere_dose', 'vaccins_premiere_dose_moyenne_quotidienne', 'vaccins_vaccines','vaccins_vaccines_moyenne_quotidienne','taux_occupation','facteur_reproduction', 'couverture_vaccinale_dose1', 'couverture_vaccinale_complet']

kpis = ['couverture_vaccinale_dose1', 'couverture_vaccinale_complet']

log.debug(kpis)

res = {}

if('taux_incidence' in kpis):
    get_taux('taux_incidence', True)

if('taux_positivite' in kpis):
    get_taux('taux_positivite', True)

if('cas_positifs' in kpis):
    get_kpi('cas_positifs', 'pos_7j', False)

if('hospitalisations' in kpis):
    get_kpi('hospitalisations', 'hosp', False)

if('hospitalisations_moyenne_quotidienne' in kpis):
    get_kpi('hospitalisations_moyenne_quotidienne', 'incid_hosp', True)

if('soins_critiques' in kpis):
    get_kpi('soins_critiques', 'rea', False)

if('soins_critiques_moyenne_quotidienne' in kpis):
    get_kpi('soins_critiques_moyenne_quotidienne', 'incid_rea', True)

if('deces' in kpis):
    get_kpi('deces', 'dchosp', False) 

if('deces_moyenne_quotidienne' in kpis):
    get_kpi('deces_moyenne_quotidienne', 'incid_dchosp', True)

if('deces_total' in kpis):
    get_kpi_only_france('deces_total', 'dc_tot', False)

if('retour_a_domicile' in kpis):
    get_kpi('retour_a_domicile', 'rad', False)

if('retour_a_domicile_moyenne_quotidienne' in kpis):
    get_kpi('retour_a_domicile_moyenne_quotidienne', 'incid_rad', True)

if('vaccins_premiere_dose' in kpis):
    get_kpi('vaccins_premiere_dose', 'n_cum_dose1', False, True)

if('vaccins_premiere_dose_moyenne_quotidienne' in kpis):
    get_kpi('vaccins_premiere_dose_moyenne_quotidienne', 'n_dose1', True, True)

if('vaccins_vaccines' in kpis):
    get_kpi('vaccins_vaccines','n_cum_complet', False, True)

if('vaccins_vaccines_moyenne_quotidienne' in kpis):
    get_kpi('vaccins_vaccines_moyenne_quotidienne', 'n_complet', True, True)

if('taux_occupation' in kpis):
    get_taux_specific('taux_occupation', 'TO')

if('facteur_reproduction' in kpis):
    get_taux_specific('facteur_reproduction', 'R')

if('couverture_vaccinale_dose1' in kpis):
    get_taux('couverture_vaccinale_dose1', False)

if('couverture_vaccinale_complet' in kpis):
    get_taux('couverture_vaccinale_complet', False)


shorten_and_save(kpis)

save_new_files()
