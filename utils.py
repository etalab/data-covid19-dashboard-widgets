import toml
from datetime import datetime
from datetime import timedelta
import time


def getColor(val,trendType):
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

def getEmptyIndicateur():
    indicateurResult = {}
    indicateurResult['nom'] = []
    indicateurResult['unite'] = []
    indicateurResult['france'] = []
    indicateurResult['regions'] = []
    indicateurResult['departements'] = []
    return indicateurResult


def getConfig(group):
    config = toml.load('./config.toml')
    return config[group]

def formatDict(last_value,last_date,evol,evol_percentage,level,code_level,dfvalues,column,trendType):  
    resdict = {}
    resdict['last_value'] = str(last_value)
    resdict['last_date'] = str(last_date)
    resdict['evol'] = str(evol)
    resdict['evol_percentage'] = str(round(evol_percentage,2))
    resdict['evol_color'] =  getColor(evol_percentage,trendType)
    resdict['level'] = level
    resdict['code_level'] = str(code_level)
    resdict['values'] = []
    for index, row in dfvalues.iterrows():
        interdict = {}
        interdict['value'] = str(row[column])
        interdict['date'] = row['date']
        resdict['values'].append(interdict)
    return resdict




def getMeanKPI(date,df,column):
    x = 0
    cpt = 0
    lowestDate  = datetime.strftime(datetime.strptime(date, "%Y-%m-%d")- timedelta(days=6),"%Y-%m-%d")
    return df[(df['date'] >= lowestDate) & (df['date'] <= date)].mean()[column].mean().round(0)

def datasetSyntheseProcessing(df, level, code_level, trendType, column, shorten=False):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    if(shorten):
        df = df[:-3]

    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x][column].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol'] = df[column] - df['7days_ago']
    df['evol_percentage'] = df['evol'] / df['7days_ago'] * 100

    return formatDict(
        df[df['date'] == df.date.max()][column].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol'].iloc[0],
        df[df['date'] == df.date.max()]['evol_percentage'].iloc[0],
        level,
        code_level,
        df[[column,'date']],
        column,
        trendType
    )  


def datasetSyntheseRollingMeanProcessing(df, level, code_level, trendType, column):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    
    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['mean'] = df['date'].apply(lambda x: getMeanKPI(x,df,column))
    df['mean_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['mean'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_mean'] = df['mean'] - df['mean_7days_ago']    
    df['evol_mean_percentage'] = df['evol_mean'] / df['mean_7days_ago'] * 100

    return formatDict(
        df[df['date'] == df.date.max()]['mean'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_mean'].iloc[0],
        df[df['date'] == df.date.max()]['evol_mean_percentage'].iloc[0],
        level,
        code_level,
        df[['mean','date']],
        'mean',
        trendType
    )  



def casPositifsProcessing(df, level, code_level, trendType):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    df = df[:-3]
    df = df[df['pos_7j'].notna()]
    df['pos7j_moyen'] = df['pos_7j'] / 7
    df['pos7j_moyen'] = df['pos7j_moyen'].astype(int)

    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['pos7j_moyen_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['pos7j_moyen'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)

    df['evol_pos7j_moyen'] = df['pos7j_moyen'] - df['pos7j_moyen_7days_ago']
    df['evol_pos7j_moyen_percentage'] = df['evol_pos7j_moyen'] / df['pos7j_moyen_7days_ago'] * 100

    
    return formatDict(
        int(df[df['date'] == df.date.max()]['pos7j_moyen'].iloc[0]),
        df.date.max(),
        int(df[df['date'] == df.date.max()]['evol_pos7j_moyen'].iloc[0]),
        df[df['date'] == df.date.max()]['evol_pos7j_moyen_percentage'].iloc[0],
        level,
        code_level,
        df[['pos7j_moyen','date']],
        'pos7j_moyen',
        trendType
    )  
    

