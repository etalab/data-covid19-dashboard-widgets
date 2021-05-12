# Processing des données Covid

- Récupération des données open data sur data.gouv
- Vérification de leur validité
- Si les données sont nouvelles, mise en route du processing
- Processing de plusieurs KPIs (liste dans le fichier config.toml). Pour certains, des traitements spécifiques sont appliqués (ex: moyenne glissante)
- Publication des données de sorties au format json dans le dossier dist.

## Exécution des scripts

Processing des KPIs :

```
python main.py
```

Génération de graphiques :

```
python plot_kpi.py
```

Tweet (nécessité d'avoir les secrets api_key, api_secret_key, token, secret_token de son compte Twitter) :

```
python tweet.py
```

## CI

CircleCI exécute toutes les heures de 17h à 21h le script principal.

## Ajouter un KPI

- Ajouter un item dans le fichier config.toml
- Ajouter le fichier source dans le dictionnaire ```urls``` du fichier download_and_check_files.py avec le nom du KPI et les colonnes concernées.
- Configurer le bon process dans le fichier main.py. 

## Les process

- récupération de données stocks
- calcul de moyennes glissantes
- spécificité pour certains KPIs (taux occupation, facteur reproduction, décès total)


