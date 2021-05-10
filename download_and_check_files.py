import requests
import filecmp
from shutil import copyfile
import glob
import pandas as pd

url = "https://www.data.gouv.fr/fr/datasets/r/f335f9ea-86e3-4ffa-9684-93c009d5e617"

def check_files(title,columns):
    df = pd.read_csv('files_new/'+title+".csv", sep=None, engine='python',dtype={'reg':str,'dep':str})
    for column in columns:
        if column not in df.columns:
            return False
    if(df.shape[0] == 0):
        return False
    return True

def download_and_check():
    urls = [
        {
            "res_id": "5c4e1452-3850-4b59-b11c-3dd51d7fb8b5",
            "title": "synthese",
            "kpis": ["hospitalisations","hospitalisations_moyenne_quotidienne","retour_a_domicile","retour_a_domicile_moyenne_quotidienne","soins_critiques","soins_critiques_moyenne_quotidienne","deces","deces_moyenne_quotidienne","cas_positifs"],
            "columns": ["dep","date","pos_7j","hosp","incid_hosp","rea","incid_rea","dchosp","incid_dchosp","rad","incid_rad","R","TO"]
        },
        {
            "res_id": "f335f9ea-86e3-4ffa-9684-93c009d5e617",
            "title": "synthese_fra",
            "kpis": ["deces_total"],
            "columns": ["date","dc_tot","R","TO"]
        },
        {
            "res_id": "cbd6477e-bda6-485d-afdc-8e61b904d771",
            "title": "taux_fra",
            "kpis": ["taux_incidence","taux_positivite"],
            "columns": ["semaine_glissante","P","pop","T"]
            
        },
        {
            "res_id": "36ab36ea-d073-4543-9ea0-a64a0175083f",
            "title": "taux_reg",
            "kpis": ["taux_incidence","taux_positivite"],
            "columns": ["semaine_glissante","reg","P","pop","T"]
        },
        {
            "res_id": "3c18e242-7d45-44f2-ac70-dee78a38ee1c",
            "title": "taux_dep",
            "kpis": ["taux_incidence","taux_positivite"],
            "columns": ["semaine_glissante","dep","P","pop","T"]
        },
        {
            "res_id": "4f39ec91-80d7-4602-befb-4b522804c0af",
            "title": "vaccin",
            "kpis": ["vaccins_premiere_dose","vaccins_premiere_dose_moyenne_quotidienne","vaccins_vaccines"],
            "columns": ["jour","dep","n_cum_dose1","n_cum_complet","n_dose1"]
        }
    ]

    output_kpis = []

    for url in urls:
        r = requests.get("https://www.data.gouv.fr/fr/datasets/r/"+url['res_id'])  
        with open('./files_new/'+url['title']+'.csv', 'wb') as f:
            f.write(r.content)
        comp = filecmp.cmp('./files_new/'+url['title']+'.csv', './files_previous/'+url['title']+'.csv')

        if comp:
            print("File '"+url['title']+"' not updated since last check")
        else:
            check = check_files(url['title'],url['columns'])
            if(check):
                print("File '"+url['title']+"' updated and tests passed, need to be processed")
                output_kpis = output_kpis + url['kpis']
            else:
                print("File '"+url['title']+"' updated but tests not passed, do not process")


    output_kpis = list(dict.fromkeys(output_kpis))

    return output_kpis
    ## Execute main


def save_new_files():
    files = glob.glob("files_new/*.csv")
    for file in files:
        copyfile(file, 'files_previous/'+file.replace('files_new/',''))
