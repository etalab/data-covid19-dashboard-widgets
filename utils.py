import toml
from datetime import datetime
from datetime import timedelta
import time
import pandas as pd
from tqdm import tqdm
import json
import glob

deps = pd.read_csv('utils/departement2021.csv',dtype=str)
deps = deps[['DEP','REG']]
deps = deps.rename(columns={'DEP':'dep','REG':'reg'})
deps = deps.drop_duplicates(keep="first")

countries = ["National"]

def get_color(val,trendType):
    if(val > 0):
        if(trendType == 'normal'):
            color = 'red'
        else:
            color = 'green'
    else:
        if(trendType == 'normal'):
            color = 'green'
        else:
            color = 'red'
    return color

def get_empty_kpi():
    indicateurResult = {}
    indicateurResult['nom'] = []
    indicateurResult['unite'] = []
    indicateurResult['france'] = []
    indicateurResult['regions'] = []
    indicateurResult['departements'] = []
    return indicateurResult


def get_config(group):
    config = toml.load('./config.toml')
    return config[group]

def format_dict(last_value,last_date,evol,evol_percentage,level,code_level,dfvalues,column,trendType):  
    resdict = {}
    resdict['last_value'] = str(last_value)
    resdict['last_date'] = str(last_date)
    resdict['evol'] = str(evol)
    resdict['evol_percentage'] = str(round(evol_percentage,2))
    resdict['evol_color'] =  get_color(evol_percentage,trendType)
    resdict['level'] = level
    resdict['code_level'] = str(code_level)
    resdict['values'] = []
    for index, row in dfvalues.iterrows():
        interdict = {}
        interdict['value'] = str(round(row[column],2))
        interdict['date'] = row['date']
        resdict['values'].append(interdict)
    return resdict


def get_rolling_average(date,df,column):
    lowestDate  = datetime.strftime(datetime.strptime(date, "%Y-%m-%d")- timedelta(days=6),"%Y-%m-%d")
    return df[(df['date'] >= lowestDate) & (df['date'] <= date)].mean()[column].mean().round(0)

def process_stock(df, level, code_level, trendType, column, shorten=False):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    if(shorten):
        df = df[:-3]

    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x][column].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol'] = df[column] - df['7days_ago']
    df['evol_percentage'] = df['evol'] / df['7days_ago'] * 100

    return format_dict(
        round(df[df['date'] == df.date.max()][column].iloc[0],2),
        df.date.max(),
        round(df[df['date'] == df.date.max()]['evol'].iloc[0],2),
        df[df['date'] == df.date.max()]['evol_percentage'].iloc[0],
        level,
        code_level,
        df[[column,'date']],
        column,
        trendType
    )  


def process_rolling_average(df, level, code_level, trendType, column):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    
    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['mean'] = df['date'].apply(lambda x: get_rolling_average(x,df,column))
    df['mean_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['mean'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_mean'] = df['mean'] - df['mean_7days_ago']    
    df['evol_mean_percentage'] = df['evol_mean'] / df['mean_7days_ago'] * 100

    return format_dict(
        int(df[df['date'] == df.date.max()]['mean'].iloc[0]),
        df.date.max(),
        int(df[df['date'] == df.date.max()]['evol_mean'].iloc[0]),
        df[df['date'] == df.date.max()]['evol_mean_percentage'].iloc[0],
        level,
        code_level,
        df[['mean','date']],
        'mean',
        trendType
    )  


def save_result(res,name):
    with open('dist/'+name+'.json','w') as fp:
        json.dump(res, fp)


def enrich_dataframe(df,name):
    if(name == 'taux_incidence'):
        df['taux_incidence'] = df['P']*100000/df['pop']
    if(name == 'taux_positivite'):
        df['taux_positivite'] = df['P']/df['T']* 100
    if(name == 'taux_occupation'):
        df['TO'] = df['TO']*100
    return df

def get_taux(name):
    indicateurResult = get_empty_kpi()
    config = get_config(name)
    print('Processing - '+name)

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
    
    df = pd.read_csv('files_new/'+config['res_id_fra'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df = enrich_dataframe(df,name)
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    for country in tqdm(countries, desc="Processing National"):
        res = process_stock(df,'nat','fra', config['trendType'],name)
        indicateurResult['france'].append(res)

    df = pd.read_csv('files_new/'+config['res_id_reg'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df = enrich_dataframe(df,name)
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):
        res = process_stock(df[df['reg'] == reg].copy(),'reg',reg, config['trendType'],name)
        indicateurResult['regions'].append(res)

    df = pd.read_csv('files_new/'+config['res_id_dep'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df = enrich_dataframe(df,name)
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = process_stock(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],name)
        indicateurResult['departements'].append(res)
    
    save_result(indicateurResult,name)




def get_kpi_by_type(df, level, code_level, trendType, column, mean):
    if(mean):
        res = process_rolling_average(df,level, code_level, trendType,column)
    else:
        res = process_stock(df,level, code_level, trendType,column)
    return res

def get_kpi(name, column, mean, transformDF=False):
    indicateurResult = get_empty_kpi()
    config = get_config(name)
    print('Processing - '+name)

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
    
    df = pd.read_csv('files_new/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    
    # Specificity KPI vaccins
    if(transformDF):
        df = df.rename(columns={'jour':'date'})
        df = pd.merge(df,deps,on='dep',how='left')
        df = df[df['reg'].notna()]
    
    # Specifity cas_positifs
    if(column == 'pos_7j'):
        df = df[df['pos_7j'].notna()]
        df['pos7j_moyen'] = df['pos_7j'] / 7
        df['pos7j_moyen'] = df['pos7j_moyen'].astype(int)
        column = 'pos7j_moyen'

    df = df[['dep','reg','date',column]]
    for country in tqdm(countries, desc="Processing National"):
        res = get_kpi_by_type(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],column,mean)
        indicateurResult['france'].append(res)

    dfinter = df.groupby(['date','reg'],as_index=False).sum()
    for reg in tqdm(df.reg.unique(),desc="Processing Régions"): 
        res = get_kpi_by_type(dfinter[dfinter['reg'] == reg].copy(),'reg',reg, config['trendType'],column,mean)
        indicateurResult['regions'].append(res)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = get_kpi_by_type(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],column,mean)
        indicateurResult['departements'].append(res)    

    save_result(indicateurResult,name)



def get_kpi_only_france(name, column, mean, transformDF=False):
    indicateurResult = get_empty_kpi()
    config = get_config(name)
    print('Processing - '+name)

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
    
    df = pd.read_csv('files_new/'+config['res_id'], sep=None, engine='python',dtype={'reg':str,'dep':str})

    df = df[['date',column]]
    for country in tqdm(countries, desc="Processing National"):
        res = get_kpi_by_type(df.groupby(['date'],as_index=False).sum(),'nat','fra', config['trendType'],column,mean)
        indicateurResult['france'].append(res)

    save_result(indicateurResult,name)



def get_taux_specific(name, column):
    indicateurResult = get_empty_kpi()
    config = get_config(name)
    print('Processing - '+name)

    indicateurResult['nom'] = config['nom']
    indicateurResult['unite'] = config['unite']
    
    df = pd.read_csv('files_new/'+config['res_id_fra'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df = df[df[column].notna()]
    df = df.sort_values(by=['date'])
    df = enrich_dataframe(df,name)

    for country in tqdm(countries, desc="Processing National"):
        res = process_stock(df,'nat','fra', config['trendType'],column)
        indicateurResult['france'].append(res)

    df = pd.read_csv('files_new/'+config['res_id_reg'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df = df[df[column].notna()]
    df = df.sort_values(by=['date'])
    df = enrich_dataframe(df,name)

    for reg in tqdm(df.reg.unique(),desc="Processing Régions"):
        res = process_stock(df[df['dep'] == df[df['reg'] == reg].dep.unique()[0]].copy(),'reg',reg, config['trendType'],column)
        indicateurResult['regions'].append(res)

    df = pd.read_csv('files_new/'+config['res_id_dep'], sep=None, engine='python',dtype={'reg':str,'dep':str})
    df = df[df[column].notna()]
    df = df.sort_values(by=['date'])
    df = enrich_dataframe(df,name)

    for dep in tqdm(df.dep.unique(),desc="Processing Départements"):
        res = process_stock(df[df['dep'] == dep].copy(),'dep',dep, config['trendType'],column)
        indicateurResult['departements'].append(res)
    
    save_result(indicateurResult,name)


def shorten_and_save(kpis):
    for kpi in kpis:
        with open('./dist/'+kpi+'.json') as f:
            data = json.load(f)
        if(data['france'] != None):
            data['france'][0].pop('values')
        if(data['regions'] != None):
            for reg in data['regions']:
                reg.pop('values')
        if(data['departements'] != None):
            for dep in data['departements']:
                dep.pop('values')
        with open("./dist/"+kpi+"_short.json", "w") as write_file:
            json.dump(data, write_file)
