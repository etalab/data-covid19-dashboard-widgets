
import pandas as pd
from tqdm import tqdm

from utils import *

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
    df['tx_incidence'] = df['P']*100000/df['pop']
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    for country in tqdm(countries, desc="Processing National"):
        res = datasetSyntheseProcessing(df,'nat','fra', config['trendType'],'tx_incidence')
        indicateurResult['france'].append(res)

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_reg'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df['tx_incidence'] = df['P']*100000/df['pop']
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):
        res = datasetSyntheseProcessing(df[df['reg'] == reg].copy(),'reg',reg, config['trendType'],'tx_incidence')
        indicateurResult['regions'].append(res)

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_dep'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df['tx_incidence'] = df['P']*100000/df['pop']
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'tx_incidence')
        indicateurResult['departements'].append(res)
        
    return indicateurResult


def getTauxPositivite():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('taux_positivite')
    print('Processing - Taux positivité')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
    
    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_fra'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df['tx_positivite'] = df['P']/df['T']* 100
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    for country in tqdm(countries, desc="Processing National"):
        res = datasetSyntheseProcessing(df,'nat','fra', config['trendType'],'tx_positivite')
        indicateurResult['france'].append(res)

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_reg'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df['tx_positivite'] = df['P']/df['T']* 100
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):
        res = datasetSyntheseProcessing(df[df['reg'] == reg].copy(),'reg',reg, config['trendType'],'tx_positivite')
        indicateurResult['regions'].append(res)

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id_dep'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df['tx_positivite'] = df['P']/df['T']* 100
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'tx_positivite')
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
        res = datasetSyntheseProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'hosp')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = datasetSyntheseProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'hosp')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'hosp')
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
        res = datasetSyntheseRollingMeanProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'incid_hosp')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = datasetSyntheseRollingMeanProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'incid_hosp')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseRollingMeanProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'incid_hosp')
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
        res = datasetSyntheseProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'rea')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = datasetSyntheseProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'rea')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'rea')
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
        res = datasetSyntheseRollingMeanProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'incid_rea')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = datasetSyntheseRollingMeanProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'incid_rea')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseRollingMeanProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'incid_rea')
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
        df = df[['dep','reg','date','dchosp']]
        res = datasetSyntheseProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'dchosp')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = datasetSyntheseProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'dchosp')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'dchosp')
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
        res = datasetSyntheseRollingMeanProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'incid_dchosp')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = datasetSyntheseRollingMeanProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'incid_dchosp')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseRollingMeanProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'incid_dchosp')
        indicateurResult['departements'].append(res)    
    
    return indicateurResult




def getRad():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('retour_a_domicile')
    print('Processing - retour_a_domicile')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
        
    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        df = df[['dep','reg','date','rad','incid_rad']]
        res = datasetSyntheseProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'rad')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = datasetSyntheseProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'rad')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'rad')
        indicateurResult['departements'].append(res)    

    return indicateurResult


def getMeanRad():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('retour_a_domicile_moyenne_quotidienne')
    print('Processing - retour_a_domicile_moyenne_quotidienne')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
        
    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    for country in tqdm(countries, desc="Processing National"):
        df = df[['dep','reg','date','incid_rad','rad']]
        res = datasetSyntheseRollingMeanProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'incid_rad')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):    
        res = datasetSyntheseRollingMeanProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'incid_rad')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseRollingMeanProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'incid_rad')
        indicateurResult['departements'].append(res)    

    return indicateurResult



def getFirstDoseVaccins():
    
    indicateurResult = getEmptyIndicateur()
    config = getConfig('vaccins_premiere_dose')
    print('Processing - Vaccins')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df = df[['dep','jour','n_cum_dose1']]
    df = df.rename(columns={'jour':'date'})
    df = pd.merge(df,deps,on='dep',how='left')

    for country in tqdm(countries, desc="Processing National"):
        res = datasetSyntheseProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'n_cum_dose1')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(dfinter.reg.unique(),desc="Processing Régions"):
        res = datasetSyntheseProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'n_cum_dose1')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'n_cum_dose1')
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
        res = datasetSyntheseProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'n_cum_complet')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(dfinter.reg.unique(),desc="Processing Régions"):
        res = datasetSyntheseProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'n_cum_complet')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'n_cum_complet')
        indicateurResult['departements'].append(res)    
        
    return indicateurResult


def getMeanFirstDoseVaccins():
    indicateurResult = getEmptyIndicateur()
    config = getConfig('vaccins_premiere_dose_moyenne')
    print('Processing - Vaccins moyennes quotidiennes')

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']

    df = pd.read_csv('https://www.data.gouv.fr/fr/datasets/r/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df = df[['dep','jour','n_dose1']]
    df = df.rename(columns={'jour':'date'})
    df = pd.merge(df,deps,on='dep',how='left')

    for country in tqdm(countries, desc="Processing National"):
        res = datasetSyntheseRollingMeanProcessing(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],'n_dose1')
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(dfinter.reg.unique(),desc="Processing Régions"):
        res = datasetSyntheseRollingMeanProcessing(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],'n_dose1')
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = datasetSyntheseRollingMeanProcessing(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],'n_dose1')
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

