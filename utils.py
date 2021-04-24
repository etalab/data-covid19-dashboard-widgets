import toml
from datetime import datetime
from datetime import timedelta


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

def formatDict(last_value,last_date,evol,evol_percentage,level,code_level,dfvalues,columns,trendType):  
    resdict = {}
    resdict['last_value'] = []
    for item in last_value:
        resdict['last_value'].append(str(item))
    resdict['last_date'] = last_date
    resdict['evol'] = []
    for item in evol:
        resdict['evol'].append(str(item))
    resdict['evol_percentage'] = []
    for item in evol_percentage:
        resdict['evol_percentage'].append(str(round(item,2)))
    resdict['evol_color'] = []
    for item in evol_percentage:
        resdict['evol_color'] = getColor(item,trendType)
    resdict['level'] = level
    resdict['code_level'] = code_level
    resdict['values'] = []
    for index, row in dfvalues.iterrows():
        interdict = {}
        interdict['value'] = []
        for item in columns:
            interdict['value'].append(str(row[item]))
        interdict['date'] = row['date']
        resdict['values'].append(interdict)
    return resdict


def getMeanKPI(date,df,column):
    x = 0
    cpt = 0
    for i in range(0,7):
        newDate  = datetime.strftime(datetime.strptime(date, "%Y-%m-%d")- timedelta(days=i),"%Y-%m-%d")
        if(df[df['date'] == newDate].shape[0] > 0):
            x = x + df[df['date'] == newDate][column].iloc[0]
            cpt = cpt + 1
    x = x / cpt
    return x
