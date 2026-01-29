# 📊 Prisme – Analyse ETF pour la Gestion de Patrimoine

## 🧠 Contexte du projet

**Prisme** est un projet de data analysis appliqué à la **gestion de patrimoine**.  
Il s’inscrit dans le cadre d’un **cabinet fictif de Conseil en Gestion de Patrimoine (CGP)** dont l’objectif est d’aider à la **prise de décision d’investissement** à partir de données financières historiques d’ETF.

Le projet ne cherche **pas à prédire les marchés**, mais à **structurer une stratégie d’investissement rationnelle**, cohérente et adaptée au **profil de risque du client**.

---

## 🎯 Objectifs

- Analyser les performances historiques des ETF  
- Mesurer le risque (volatilité, dispersion des rendements)  
- Comparer différents marchés (États-Unis, Europe, marchés émergents)  
- Segmenter les profils investisseurs (défensif, équilibré, dynamique)  
- Proposer des recommandations patrimoniales argumentées  
- Mettre à disposition une application Streamlit interactive  

---

## 👤 Profil client fictif

**Pierre**, 33 ans  
- Horizon d’investissement : long terme (20–30 ans)  
- Objectif : constitution de patrimoine  
- Tolérance au risque : modérée à dynamique  
- Stratégie privilégiée : investissement via ETF diversifiés  

---

## 📁 Données utilisées

- Données financières historiques d’ETF (Open, High, Low, Close, Volume, Date)  
- Données descriptives ETF (devise, société de gestion, encours, performance YTD)  
- Format de stockage : **Parquet**  

Le format Parquet est particulièrement adapté à l’analyse financière :
- performant  
- structuré  
- optimisé pour le traitement de volumes importants  

---

## 🧪 Méthodologie

1. Collecte et stockage des données financières  
2. Nettoyage et normalisation des données  
3. Calcul des indicateurs clés :
   - rendements  
   - volatilité  
   - performance cumulée  
4. Analyse comparative des ETF  
5. Segmentation des profils investisseurs  
6. Restitution via une application Streamlit  

⚠️ Les performances passées sont utilisées uniquement comme **outil d’analyse**, et non comme promesse de performance future.

---

## ⚖️ Analyse du risque

Le risque est évalué principalement à travers :
- la volatilité des rendements  
- la régularité des performances  
- la corrélation entre ETF (diversification)  

Cette approche permet d’adapter les allocations en fonction de la **tolérance au risque du client**.

---

## 🖥️ Application Streamlit

L’application permet :
- de visualiser la performance cumulée des ETF  
- de comparer risque et rendement  
- de filtrer les ETF analysés  
- de proposer des recommandations selon le profil investisseur  
- d’offrir une lecture pédagogique adaptée à un CGP  

### Lancer l’application
```bash
streamlit run src/streamlit-app.py
```
ou
```bash
python main.py
```




