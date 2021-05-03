from datetime import timedelta
from datetime import datetime
from utils import getColor, formatDict, getMeanKPI
import time

def tauxIncidenceProcessing(df,level,code_level, trendType):
    df['tx_incidence'] = df['P']*100000/df['pop']
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['tx_incidence_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['tx_incidence'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_tx_incidence'] = df['tx_incidence'] - df['tx_incidence_7days_ago']
    df['evol_tx_incidence_percentage'] = df['evol_tx_incidence'] / df['tx_incidence_7days_ago'] * 100
    
    return formatDict(
        df[df['date'] == df.date.max()]['tx_incidence'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_tx_incidence'].iloc[0],
        df[df['date'] == df.date.max()]['evol_tx_incidence_percentage'].iloc[0],
        level,
        code_level,
        df[['tx_incidence','date']],
        'tx_incidence',
        trendType
    )  

def hospProcessing(df, level, code_level, trendType):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)

    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['hosp_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['hosp'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_hosp'] = df['hosp'] - df['hosp_7days_ago']
    df['evol_hosp_percentage'] = df['evol_hosp'] / df['hosp_7days_ago'] * 100

    return formatDict(
        df[df['date'] == df.date.max()]['hosp'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_hosp'].iloc[0],
        df[df['date'] == df.date.max()]['evol_hosp_percentage'].iloc[0],
        level,
        code_level,
        df[['hosp','date']],
        'hosp',
        trendType
    )  


def hospMeanProcessing(df, level, code_level, trendType):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    
    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['mean_incid_hosp'] = df['date'].apply(lambda x: getMeanKPI(x,df,'incid_hosp'))
    df['mean_incid_hosp_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['mean_incid_hosp'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_mean_incid_hosp'] = df['mean_incid_hosp'] - df['mean_incid_hosp_7days_ago']    
    df['evol_mean_incid_hosp_percentage'] = df['evol_mean_incid_hosp'] / df['mean_incid_hosp_7days_ago'] * 100

    return formatDict(
        df[df['date'] == df.date.max()]['mean_incid_hosp'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_mean_incid_hosp'].iloc[0],
        df[df['date'] == df.date.max()]['evol_mean_incid_hosp_percentage'].iloc[0],
        level,
        code_level,
        df[['mean_incid_hosp','date']],
        'mean_incid_hosp',
        trendType
    )  

def reaProcessing(df,level,code_level, trendType):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['rea_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['rea'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_rea'] = df['rea'] - df['rea_7days_ago']
    df['evol_rea_percentage'] = df['evol_rea'] / df['rea_7days_ago'] * 100

    return formatDict(
        df[df['date'] == df.date.max()]['rea'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_rea'].iloc[0],
        df[df['date'] == df.date.max()]['evol_rea_percentage'].iloc[0],
        level,
        code_level,
        df[['rea','date']],
        'rea',
        trendType
    )  


def reaMeanProcessing(df,level,code_level, trendType):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)

    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['mean_incid_rea'] = df['date'].apply(lambda x: getMeanKPI(x,df,'incid_rea'))
    df['mean_incid_rea_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['mean_incid_rea'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_mean_incid_rea'] = df['mean_incid_rea'] - df['mean_incid_rea_7days_ago']
    df['evol_mean_incid_rea_percentage'] = df['evol_mean_incid_rea'] / df['mean_incid_rea_7days_ago'] * 100


    return formatDict(
        df[df['date'] == df.date.max()]['mean_incid_rea'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_mean_incid_rea'].iloc[0],
        df[df['date'] == df.date.max()]['evol_mean_incid_rea_percentage'].iloc[0],
        level,
        code_level,
        df[['mean_incid_rea','date']],
        'mean_incid_rea',
        trendType
    )  

def dcProcessing(df,level,code_level, trendType):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['incid_dchosp_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['incid_dchosp'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_incid_dchosp'] = df['incid_dchosp'] - df['incid_dchosp_7days_ago']
    df['evol_incid_dchosp_percentage'] = df['evol_incid_dchosp'] / df['incid_dchosp_7days_ago'] * 100

    return formatDict(
        df[df['date'] == df.date.max()]['incid_dchosp'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_incid_dchosp'].iloc[0],
        df[df['date'] == df.date.max()]['evol_incid_dchosp_percentage'].iloc[0],
        level,
        code_level,
        df[['incid_dchosp','date']],
        'incid_dchosp',
        trendType
    )  

def dcMeanProcessing(df,level,code_level, trendType):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
   
    df['mean_incid_dchosp'] = df['date'].apply(lambda x: getMeanKPI(x,df,'incid_dchosp'))
    df['mean_incid_dchosp_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['mean_incid_dchosp'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_mean_incid_dchosp'] = df['mean_incid_dchosp'] - df['mean_incid_dchosp_7days_ago']
    df['evol_mean_incid_dchosp_percentage'] = df['evol_mean_incid_dchosp'] / df['mean_incid_dchosp_7days_ago'] * 100


    return formatDict(
        df[df['date'] == df.date.max()]['mean_incid_dchosp'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_mean_incid_dchosp'].iloc[0],
        df[df['date'] == df.date.max()]['evol_mean_incid_dchosp_percentage'].iloc[0],
        level,
        code_level,
        df[['mean_incid_dchosp','date']],
        'mean_incid_dchosp',
        trendType
    )  


def vaccinFirstDoseProcessing(df,level,code_level, trendType):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['n_cum_dose1_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['n_cum_dose1'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_n_cum_dose1'] = df['n_cum_dose1'] - df['n_cum_dose1_7days_ago']
    df['evol_n_cum_dose1_percentage'] = df['evol_n_cum_dose1'] / df['n_cum_dose1_7days_ago'] * 100

    return formatDict(
        df[df['date'] == df.date.max()]['n_cum_dose1'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_n_cum_dose1'].iloc[0],
        df[df['date'] == df.date.max()]['evol_n_cum_dose1_percentage'].iloc[0],
        level,
        code_level,
        df[['n_cum_dose1','date']],
        'n_cum_dose1',
        trendType
    )  


def vaccinFullProcessing(df,level,code_level, trendType):
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['n_cum_complet_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['n_cum_complet'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_n_cum_complet'] = df['n_cum_complet'] - df['n_cum_complet_7days_ago']
    df['evol_n_cum_complet_percentage'] = df['evol_n_cum_complet'] / df['n_cum_complet_7days_ago'] * 100

    return formatDict(
        df[df['date'] == df.date.max()]['n_cum_complet'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_n_cum_complet'].iloc[0],
        df[df['date'] == df.date.max()]['evol_n_cum_complet_percentage'].iloc[0],
        level,
        code_level,
        df[['n_cum_complet','date']],
        'n_cum_complet',
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
    



def tauxPositiviteProcessing(df,level,code_level, trendType):
    
    df['tx_positivite'] = df['P']/df['pop']* 100
    df['date'] = df['semaine_glissante'].apply(lambda x: str(x)[11:])
    df = df.sort_values(by=['date'])
    df = df.reset_index(drop=True)
    df['date_7days_ago'] = df['date'].apply(lambda x: datetime.strftime(datetime.strptime(x, "%Y-%m-%d") - timedelta(days=7),"%Y-%m-%d"))
    df['tx_positivite_7days_ago'] = df['date_7days_ago'].apply(lambda x: df[df['date'] == x]['tx_positivite'].iloc[0] if(df[df['date'] == x].shape[0] > 0) else None)
    df['evol_tx_positivite'] = df['tx_positivite'] - df['tx_positivite_7days_ago']
    df['evol_tx_positivite_percentage'] = df['evol_tx_positivite'] / df['tx_positivite_7days_ago'] * 100
    
    return formatDict(
        df[df['date'] == df.date.max()]['tx_positivite'].iloc[0],
        df.date.max(),
        df[df['date'] == df.date.max()]['evol_tx_positivite'].iloc[0],
        df[df['date'] == df.date.max()]['evol_tx_positivite_percentage'].iloc[0],
        level,
        code_level,
        df[['tx_positivite','date']],
        'tx_positivite',
        trendType
    )  
