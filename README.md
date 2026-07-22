# IndexesDownloader

Télécharge les historiques quotidiens et mensuels EOM de huit indices européens :

- STOXX Europe 600 Gross Return et Price Return ;
- EURO STOXX 50 Gross Return et Price Return ;
- CAC All-Tradable Gross Return et Price Return ;
- CAC 40 Gross Return et Price Return.

## Construction mensuelle EOM

Chaque couple Gross Return / Price Return est traité comme une table unique :

1. jointure interne des deux historiques quotidiens sur les dates communes ;
2. sélection de la dernière date commune de chaque mois terminé ;
3. affectation des deux niveaux observés à la fin du mois calendaire ;
4. jointure externe finale des tables mensuelles disponibles.

Les niveaux source ne sont pas modifiés par le contrôle du dividende implicite. Les valeurs négatives sont consignées dans `index_histories/eom/Dividend_Validation_Warnings.csv` sans interrompre l’export.

## Tolérance aux échecs

Le script utilise `curl_cffi` avec une empreinte TLS/HTTP de navigateur. Lorsqu’une série ne peut pas être téléchargée :

- le dernier fichier quotidien publié est réutilisé lorsqu’il existe ;
- sinon, un CSV individuel avec métadonnées et en-tête est tout de même produit ;
- les consolidés sont générés avec toutes les colonnes attendues ;
- les erreurs sont consignées dans `index_histories/Download_Status.csv` ;
- les problèmes de construction des paires EOM sont consignés dans `index_histories/eom/Pair_Status.csv`.

Une panne d’une ou de toutes les sources n’empêche donc pas la production et la publication des fichiers.

## Exécution locale

```bash
python -m pip install -r requirements.txt
python Indexes.py
```

## GitHub Actions

Le workflow `Download index histories` peut être lancé manuellement depuis l’onglet **Actions** et s’exécute chaque lundi à 04:00 UTC. À la fin de chaque exécution, les fichiers de `index_histories/` sont :

1. joints à l’exécution sous forme d’artefact ;
2. commités et poussés automatiquement sur la branche `main` lorsqu’ils ont changé.
