import pandas as pd
import matplotlib.dates as mdates
import json
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import toml
from datetime import datetime

# Script for quickly plot kpis.

prop = fm.FontProperties(fname='./Marianne-Regular.otf')

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
    df['date'] = df['date'].apply(
        lambda x: pd.to_datetime(str(x), format='%Y-%m-%d')
    )

    x = df.date
    y = df.val

    plt.rcParams["figure.figsize"] = (16, 8)
    plt.rcParams['axes.facecolor'] = 'white'
    fig, ax = plt.subplots()
    ax.plot(x, y, linewidth=3, label='Cas', color="#0000FF")
    ax.set_ylabel(detail['unite'], fontproperties=prop, size="14")
    ax.set_title(detail['nom'], fontproperties=prop, size="30")

    myFmt = mdates.DateFormatter('%d/%m')
    ax.xaxis.set_major_formatter(myFmt)
    ax.fill_between(x, y, color="#0000FF")
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
    status = "Au "+datetime.strptime(data['france'][0]['last_date'],'%Y-%m-%d').strftime('%d/%m/%y')+", "+detail['nom']+str(int(round(float(data['france'][0]['last_value']),0)))+" ("+evol+"% par rapport à la semaine dernière)"
    print(status)
    text_file = open("kpis/"+itemGroup+".txt", "w")
    n = text_file.write(status)
    text_file.close()