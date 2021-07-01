import requests
import filecmp
from shutil import copyfile
import glob
import pandas as pd
import json
from logger import log


def check_files(title, columns):
    """Check if a downloaded file is valid.

    Two checks applied :
    - check if columns are present in file
    - check if length of file is not empty
    We could add more check here if we want.
    """
    df = pd.read_csv(
        'files_new/'+title+".csv",
        sep=None,
        engine='python',
        dtype={'reg': str, 'dep': str}
    )
    for column in columns:
        if column not in df.columns:
            return False
    if(df.shape[0] == 0):
        return False
    return True


def download_and_check():
    """Download and Check files present in dictionnary.

    Steps :
    - Download each file mentionned in dictionnary.
    - Apply checks
    - If file has not change from last version, do nothing
    - If file check failed, do nothing
    - If file has changed and checks passed, add KPI derived from file to list for processing
    """
    urls = [
        {
            "res_id": "5c4e1452-3850-4b59-b11c-3dd51d7fb8b5",
            "title": "synthese",
            "kpis": [
                "hospitalisations",
                "hospitalisations_moyenne_quotidienne",
                "retour_a_domicile",
                "retour_a_domicile_moyenne_quotidienne",
                "soins_critiques",
                "soins_critiques_moyenne_quotidienne",
                "deces",
                "deces_moyenne_quotidienne",
                "cas_positifs",
                "facteur_reproduction",
                "taux_occupation"
            ],
            "columns": [
                "dep",
                "date",
                "pos_7j",
                "hosp",
                "incid_hosp",
                "rea",
                "incid_rea",
                "dchosp",
                "incid_dchosp",
                "rad",
                "incid_rad",
                "R",
                "TO"
            ]
        },
        {
            "res_id": "f335f9ea-86e3-4ffa-9684-93c009d5e617",
            "title": "synthese_fra",
            "kpis": [
                "deces_total",
                "facteur_reproduction",
                "taux_occupation"
            ],
            "columns": [
                "date",
                "dc_tot",
                "R",
                "TO"
            ]
        },
        {
            "res_id": "cbd6477e-bda6-485d-afdc-8e61b904d771",
            "title": "taux_fra",
            "kpis": [
                "taux_incidence",
                "taux_positivite"
            ],
            "columns": [
                "semaine_glissante",
                "P",
                "pop",
                "T"
            ]
        },
        {
            "res_id": "36ab36ea-d073-4543-9ea0-a64a0175083f",
            "title": "taux_reg",
            "kpis": [
                "taux_incidence",
                "taux_positivite"
            ],
            "columns": [
                "semaine_glissante",
                "reg",
                "P",
                "pop",
                "T"
            ]
        },
        {
            "res_id": "3c18e242-7d45-44f2-ac70-dee78a38ee1c",
            "title": "taux_dep",
            "kpis": [
                "taux_incidence",
                "taux_positivite"
            ],
            "columns": [
                "semaine_glissante",
                "dep",
                "P",
                "pop",
                "T"
            ]
        },
        {
            "res_id": "4f39ec91-80d7-4602-befb-4b522804c0af",
            "title": "vaccin",
            "kpis": [
                "vaccins_premiere_dose",
                "vaccins_premiere_dose_moyenne_quotidienne",
                "vaccins_vaccines",
                "vaccins_vaccines_moyenne_quotidienne"
            ],
            "columns": [
                "jour",
                "dep",
                "n_cum_dose1",
                "n_cum_complet",
                "n_dose1",
                "n_complet"
            ]
        },
        {
            "res_id": "54dd5f8d-1e2e-4ccb-8fb8-eac68245befd",
            "title": "vaccin_couv_fra",
            "kpis": [
                "vaccins_vaccines_couv_majeurs",
                "vaccins_vaccines_couv_ado_majeurs"
            ],
            "columns": [
                "fra",
                'clage_vacsi',
                "jour",
                "n_dose1",
                "n_complet",
                "n_cum_dose1",
                "n_cum_complet",
                "couv_dose1",
                "couv_complet"
            ]
        },
        {
            "res_id": "c3ccc72a-a945-494b-b98d-09f48aa25337",
            "title": "vaccin_couv_reg",
            "kpis": [
                "vaccins_vaccines_couv_majeurs",
                "vaccins_vaccines_couv_ado_majeurs"
            ],
            "columns": [
                "reg",
                'clage_vacsi',
                "jour",
                "n_dose1",
                "n_complet",
                "n_cum_dose1",
                "n_cum_complet",
                "couv_dose1",
                "couv_complet"
            ]
        },
        {
            "res_id": "83cbbdb9-23cb-455e-8231-69fc25d58111",
            "title": "vaccin_couv_dep",
            "kpis": [
                "vaccins_vaccines_couv_majeurs",
                "vaccins_vaccines_couv_ado_majeurs"
            ],
            "columns": [
                "dep",
                'clage_vacsi',
                "jour",
                "n_dose1",
                "n_complet",
                "n_cum_dose1",
                "n_cum_complet",
                "couv_dose1",
                "couv_complet"
            ]
        },
        {
            "res_id": "dc103057-d933-4e4b-bdbf-36d312af9ca9",
            "title": "pop_age_fra",
            "kpis": [
                "vaccins_vaccines_couv_majeurs",
                "vaccins_vaccines_couv_ado_majeurs"
            ],
            "columns": [
                "fra",
                'clage_vacsi',
                "jour",
                "n_tot_dose1",
                "n_tot_complet",
                "pop",
                "couv_tot_dose1",
                "couv_tot_complet"
            ]
        },
        {
            "res_id": "2dadbaa7-02ae-43df-92bb-53a82e790cb2",
            "title": "pop_age_reg",
            "kpis": [
                "vaccins_vaccines_couv_majeurs",
                "vaccins_vaccines_couv_ado_majeurs"
            ],
            "columns": [
                "reg",
                'clage_vacsi',
                "jour",
                "n_tot_dose1",
                "n_tot_complet",
                "pop",
                "couv_tot_dose1",
                "couv_tot_complet"
            ]
        },
        {
            "res_id": "de4b356b-8cd9-4b9a-8878-459a62646107",
            "title": "pop_age_dep",
            "kpis": [
                "vaccins_vaccines_couv_majeurs",
                "vaccins_vaccines_couv_ado_majeurs"
            ],
            "columns": [
                "dep",
                'clage_vacsi',
                "jour",
                "n_tot_dose1",
                "n_tot_complet",
                "pop",
                "couv_tot_dose1",
                "couv_tot_complet"
            ]
        }
        
                
        
    ]

    output_kpis = []

    for url in urls:
        r = requests.get(
            "https://www.data.gouv.fr/fr/datasets/r/"+url['res_id']
        )
        with open('./files_new/'+url['title']+'.csv', 'wb') as f:
            f.write(r.content)
        comp = filecmp.cmp(
            './files_new/'+url['title']+'.csv',
            './files_previous/'+url['title']+'.csv'
        )

        if comp:
            log.debug('File "'+url['title']+'" not updated since last check')
        else:
            check = check_files(url['title'], url['columns'])
            if(check):
                log.debug('File "'+url['title']+'" updated and tests passed, need to be processed')
                output_kpis = output_kpis + url['kpis']
            else:
                log.error('File "'+url['title']+'" updated but tests not passed, do not process')

    output_kpis = list(dict.fromkeys(output_kpis))

    return output_kpis


def save_new_files():
    """Save new files once processed is finished.

    When KPIs processing is finished, replace previous
    files by new ones.
    """
    files = glob.glob("files_new/*.csv")
    for file in files:
        copyfile(file, 'files_previous/'+file.replace('files_new/', ''))


def shorten_and_save(kpis):
    """Save short version of kpis files.

    For each kpis, save another file without time series.
    """
    for kpi in kpis:
        with open('./dist/'+kpi+'.json') as f:
            data = json.load(f)
        if(data['france'] is not None):
            data['france'][0].pop('values')
        if(data['regions'] is not None):
            for reg in data['regions']:
                reg.pop('values')
        if(data['departements'] is not None):
            for dep in data['departements']:
                dep.pop('values')
        with open("./dist/"+kpi+"_short.json", "w") as write_file:
            json.dump(data, write_file)
