# IndexesDownloader

Télécharge huit historiques d’indices européens en parallèle, puis produit des exports quotidiens et mensuels de fin de mois.

## Séries couvertes

- STOXX Europe 600 Gross Return et Price Return
- EURO STOXX 50 Gross Return et Price Return
- CAC All-Tradable Gross Return et Price Return
- CAC 40 Gross Return et Price Return

## Méthode EOM

Pour chaque couple Gross Return / Price Return :

1. jointure interne des historiques quotidiens sur les dates communes ;
2. sélection de la dernière date commune de chaque mois terminé ;
3. affectation des niveaux observés à la fin du mois calendaire ;
4. jointure finale des quatre tables mensuelles sur les dates EOM.

Les niveaux publiés ne sont ni corrigés, ni lissés, ni rééchelonnés.

## Exécution locale

```bash
python -m pip install -r requirements.txt
python Indexes.py
```

Les fichiers sont générés dans `index_histories/`.

## GitHub Actions

Dans l’onglet **Actions**, sélectionner **Download index histories**, puis **Run workflow**. Les CSV générés sont disponibles dans un artefact nommé `index-histories`.
