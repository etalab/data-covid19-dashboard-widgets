
import pandas as pd
from tqdm import tqdm

from func import tauxIncidenceProcessing, hospProcessing, reaProcessing, firstInjectionProcessing, casPositifsProcessing
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

    for nom in config['nom']:
        indicateurResult['nom'].append(nom)
    for unite in config['unite']:
        indicateurResult['unite'].append(unite)
    
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

    for nom in config['nom']:
        indicateurResult['nom'].append(nom)
    for unite in config['unite']:
        indicateurResult['unite'].append(unite)
        
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



def getReas():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('reanimations')
    print('Processing - Réanimations')

    for nom in config['nom']:
        indicateurResult['nom'].append(nom)
    for unite in config['unite']:
        indicateurResult['unite'].append(unite)

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


def getVaccins():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('vaccins')
    print('Processing - Vaccins')

    for nom in config['nom']:
        indicateurResult['nom'].append(nom)
    for unite in config['unite']:
        indicateurResult['unite'].append(unite)

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        df = df[['dep','jour','n_cum_dose1']]
        df = df.rename(columns={'jour':'date'})
        df = pd.merge(df,deps,on='dep',how='left')
        res = firstInjectionProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'])
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(dfinter.reg.unique(),desc="Processing Régions"):
        res = firstInjectionProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'])
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = firstInjectionProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'])
        indicateurResult['departements'].append(res)    
        
    return indicateurResult


def getCasPositifs():
    indicateurResult = getEmptyIndicateur()
    config = getConfig('cas_positifs')
    print('Processing - Cas Positifs')

    for nom in config['nom']:
        indicateurResult['nom'].append(nom)
    for unite in config['unite']:
        indicateurResult['unite'].append(unite)

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