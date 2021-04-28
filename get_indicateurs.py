
import pandas as pd
from tqdm import tqdm

from func import *
from utils import getEmptyIndicateur, getConfig

deps = pd.read_csv('utils/departement2021.csv',dtype=str)
deps = deps[['DEP','REG']]
deps = deps.rename(columns={'DEP':'dep','REG':'reg'})
deps = deps.drop_duplicates(keep="first")

countries = ["National"]

def getTauxIncidence():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('taux_incidence')
    print('Processing - Taux incidence')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
    
    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_fra'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        res = tauxIncidenceProcessing(df,'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_reg'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):
        res = tauxIncidenceProcessing(df[df['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_dep'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = tauxIncidenceProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)
        
    return indicateurResult


def getHospitalisations():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('hospitalisations')
    print('Processing - Hospitalisations')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
        
    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        df = df[['dep','reg','date','incid_hosp','hosp']]
        res = hospProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = hospProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = hospProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)    

    return indicateurResult


def getMeanHospitalisations():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('hospitalisations_moyenne_quotidien')
    print('Processing - Hospitalisations Moyenne quotidien')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
        
    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        df = df[['dep','reg','date','incid_hosp','hosp']]
        res = hospMeanProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = hospMeanProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = hospMeanProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)    

    return indicateurResult



def getReas():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('soins_critiques')
    print('Processing - Réanimations')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        df = df[['dep','reg','date','incid_rea','rea']]
        res = reaProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = reaProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = reaProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)    
    
    return indicateurResult


def getMeanReas():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('soins_critiques_moyenne_quotidien')
    print('Processing - Réanimations')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        df = df[['dep','reg','date','incid_rea','rea']]
        res = reaMeanProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = reaMeanProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = reaMeanProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)    
    
    return indicateurResult


def getDeces():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('deces')
    print('Processing - Décès')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        df = df[['dep','reg','date','incid_dchosp']]
        res = dcProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = dcProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = dcProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)    
    
    return indicateurResult

def getMeanDeces():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('deces_moyenne_quotidien')
    print('Processing - Décès')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        df = df[['dep','reg','date','incid_dchosp']]
        res = dcMeanProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = dcMeanProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = dcMeanProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)    
    
    return indicateurResult


def getFirstDoseVaccins():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('vaccins_premiere_dose')
    print('Processing - Vaccins')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df = df[['dep','jour','n_cum_dose1','n_cum_complet']]
    df = df.rename(columns={'jour':'date'})
    df = pd.merge(df,deps,on='dep',how='left')

    for country in tqdm(countries, desc="Processing National"):
        res = vaccinFirstDoseProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(dfinter.reg.unique(),desc="Processing Régions"):
        res = vaccinFirstDoseProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = vaccinFirstDoseProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)    
        
    return indicateurResult

def getFullVaccins():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('vaccins_vaccines')
    print('Processing - Vaccins')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df = df[['dep','jour','n_cum_dose1','n_cum_complet']]
    df = df.rename(columns={'jour':'date'})
    df = pd.merge(df,deps,on='dep',how='left')

    for country in tqdm(countries, desc="Processing National"):
        res = vaccinFullProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(dfinter.reg.unique(),desc="Processing Régions"):
        res = vaccinFullProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = vaccinFullProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)    
        
    return indicateurResult


def getCasPositifs():
    indicateurResult = getEmptyIndicateur()
    config = getConfig('cas_positifs')
    print('Processing - Cas Positifs')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        df = df[['dep','reg','date','pos_7j']]
        res = casPositifsProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = casPositifsProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = casPositifsProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)    
    
    return indicateurResult


def getTauxPositivite():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('taux_positivite')
    print('Processing - Taux positivité')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
    
    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_fra'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        res = tauxPositiviteProcessing(df,'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_reg'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):
        res = tauxPositiviteProcessing(df[df['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_dep'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = tauxPositiviteProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)
        
    return indicateurResult
