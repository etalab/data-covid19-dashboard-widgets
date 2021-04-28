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
    return df[(df['date'] >= lowestDate) & (df['date'] <= date)].mean()[column].mean()