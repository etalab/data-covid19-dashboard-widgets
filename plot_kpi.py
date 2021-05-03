import pandas as pd
import json
import requests
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
prop = fm.FontProperties(fname='./Marianne-Regular.otf')
import matplotlib.dates as mdates
import toml
from datetime import datetime

config = toml.load('./config.toml')
for itemGroup, detail in config.items():
    print(itemGroup)
    with open('dist/'+itemGroup+'.json') as fp:
        data = json.load(fp)
    
    # plot
    arr = []
    for item in data['france'][0]['values']:
        mydict = {}
        mydict['val'] = float(item['value'])
        mydict['date'] = item['date']
        arr.append(mydict)

    df = pd.DataFrame(arr)
    df['date'] = df['date'].apply(lambda x: pd.to_datetime(str(x), format='%Y-%m-%d'))

    x = df.date
    y = df.val

    plt.rcParams["figure.figsize"] = (16,8)
    plt.rcParams['axes.facecolor'] = 'white'
    fig, ax = plt.subplots()
    ax.plot(x, y,linewidth=3,label='Cas',color=detail['colorLine'])
    ax.set_ylabel(detail['yLabel'], fontproperties=prop,size="14")
    ax.set_title(detail['titleChart'], fontproperties=prop,size="30")

    myFmt = mdates.DateFormatter('%d/%m')
    ax.xaxis.set_major_formatter(myFmt)
    ax.fill_between(x, y, color=detail['colorZone'])
    plt.setp(ax.spines.values(), color="#ebebeb")
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    fig.suptitle("France", y=0.85)
    fig.savefig('plots/'+itemGroup+'.png', transparent=True)
    plt.close()

    # KPI
    if(float(data['france'][0]['evol_percentage']) > 0):
        evol = "+"+data['france'][0]['evol_percentage']
    else:
        evol = data['france'][0]['evol_percentage']
    status = "Au "+datetime.strptime(data['france'][0]['last_date'],'%Y-%m-%d').strftime('%d/%m/%y')+", "+detail['statusTwitter']+str(int(round(float(data['france'][0]['last_value']),0)))+" ("+evol+"% par rapport à la semaine dernière)"
    print(status)
    text_file = open("kpis/"+itemGroup+".txt", "w")
    n = text_file.write(status)
    text_file.close()