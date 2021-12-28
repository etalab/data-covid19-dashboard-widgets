import json
import glob
from utils import get_taux, get_taux_variants, get_kpi, get_kpi_2files, get_kpi_only_france, get_taux_specific, get_couv, get_vacsi_non_vacsi,\
    get_kpi_scolaire, get_kpi_3_files, make_json_periods
from download_and_check_files import download_and_check, save_new_files, shorten_and_save
from logger import log

#kpis = download_and_check()
# kpis = ['hospitalisations', 'hospitalisations_moyenne_quotidienne', 'retour_a_domicile', 'retour_a_domicile_moyenne_quotidienne', 'soins_critiques', 'soins_critiques_moyenne_quotidienne', 'deces', 'deces_moyenne_quotidienne', 'cas_positifs', 'taux_incidence', 'taux_positivite', 'vaccins_premiere_dose', 'vaccins_premiere_dose_moyenne_quotidienne', 'vaccins_vaccines','vaccins_vaccines_moyenne_quotidienne','taux_occupation','facteur_reproduction']
kpis = ['vaccins_vaccines', 'vaccins_premiere_dose']

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
    get_kpi_2files('vaccins_premiere_dose', 'n_cum_dose1', False, True)

if('vaccins_premiere_dose_moyenne_quotidienne' in kpis):
    res['vaccins_premiere_dose_moyenne_quotidienne'] = getKPIGeneric('vaccins_premiere_dose_moyenne_quotidienne','n_dose1',True,True)

if('vaccins_vaccines' in kpis):
    get_kpi_2files('vaccins_vaccines','n_cum_complet', False, True)

if('vaccins_vaccines_moyenne_quotidienne' in kpis):
    get_kpi('vaccins_vaccines_moyenne_quotidienne', 'n_complet', True, True)

if('vaccins_vaccines_couv_majeurs' in kpis):
    get_couv('vaccins_vaccines_couv_majeurs','couv_complet', 24)

if('vaccins_vaccines_couv_ado_majeurs' in kpis):
    get_couv('vaccins_vaccines_couv_ado_majeurs','couv_complet', 17)

if('taux_occupation' in kpis):
    get_taux_specific('taux_occupation', 'TO')

if('facteur_reproduction' in kpis):
    get_taux_specific('facteur_reproduction', 'R')

if('pos_test_vacsi' in kpis):
    get_vacsi_non_vacsi('pos_test_vacsi', 'nb_PCR+', 'Vaccination complète', 100000)

if('pos_test_non_vacsi' in kpis):
    get_vacsi_non_vacsi('pos_test_non_vacsi', 'nb_PCR+', 'Non-vaccinés', 100000)

if('sc_vacsi' in kpis):
    get_vacsi_non_vacsi('sc_vacsi', 'SC_PCR+', 'Vaccination complète', 1000000)

if('sc_non_vacsi' in kpis):
    get_vacsi_non_vacsi('sc_non_vacsi', 'SC_PCR+', 'Non-vaccinés', 1000000)

if('pos_test_vacsi_20ans' in kpis):
    get_vacsi_non_vacsi('pos_test_vacsi_20ans', 'nb_PCR+', 'Vaccination complète', 100000, filter = True, region = False)

if('pos_test_non_vacsi_20ans' in kpis):
    get_vacsi_non_vacsi('pos_test_non_vacsi_20ans', 'nb_PCR+', 'Non-vaccinés', 100000, filter = True, region = False)

if('sc_vacsi_20ans' in kpis):
    get_vacsi_non_vacsi('sc_vacsi_20ans', 'SC_PCR+', 'Vaccination complète', 1000000, filter = True, region = False)

if('sc_non_vacsi_20ans' in kpis):
    get_vacsi_non_vacsi('sc_non_vacsi_20ans', 'SC_PCR+', 'Non-vaccinés', 1000000, filter = True, region = False)

if('nb_classes_fermees' in kpis):
    get_kpi_scolaire('nb_classes_fermees', 'nombre_classes_fermees', False)

if('taux_classes_fermees' in kpis):
    get_kpi_scolaire('taux_classes_fermees', 'taux_classes', False)

if('nb_structures_fermees' in kpis):
    get_kpi_scolaire('nb_structures_fermees', 'nombre_structures_fermees', False)

if('taux_structures_fermees' in kpis):
    get_kpi_scolaire('taux_structures_fermees', 'taux_structures', False)

if('nb_college_lycee_vaccin' in kpis):
    get_kpi_scolaire('nb_college_lycee_vaccin', 'nombre_etablissements_avec_offre_vaccinale', False)

if('vaccins_rappel' in kpis):
    get_kpi_3_files('vaccins_rappel','n_cum_rappel', False, True)

if('vaccins_rappel_moyenne_quotidienne' in kpis):
    get_kpi_3_files('vaccins_rappel_moyenne_quotidienne', 'n_rappel', True, True)

make_json_periods()
shorten_and_save(kpis)

save_new_files()
