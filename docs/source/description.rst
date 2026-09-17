BiblioParsing description
*************************

Purpose
=======

- Collection de fonctions d’aide l'analyse d'un corpus de publications
- Bibliothèque à l'usage de développeurs en python
- Les corpus bruts sont issus des bases de données bibliographiques courantes (WoS, Scopus, HAL, PubMed...)

Interfaces
==========

- Un module de démonstration de l'utilisation de la collection de fonction est proposé
- La collection de fonctions permet :
    - L'analyse élémentaire par la redistribution des informations extraites de chaque base de données par type
        - Index des publications, auteurs, affiliations, pays, institutions, références, mots clefs par type, catégories de journal...
    - La synthèse de ces informations à partir de toutes les bases de données utilisées
        - Concatenation puis deduplication des publications en occurence multiple
        - Normalisation de certaines informations pour le retrait des doublons
    - Les informations disponibles ne sont pas toutes exploitées dans l’analyse approfondie mais sont prévues de l’être
        - Éditeur, nature du journal, auteur contact...

Input Data
==========

**La position de ces fichiers est prédéterminée dans l’arborescence du dossier de travail**

- Les variables globales spécifiques à chaque groupe de fonctions
    - Actuellement modifiables par intervention dans les modules dédiés du programme
    - En cours de basculement dans des fichiers « texte » structurés (yaml ou json)
- Fichiers d’extraction des bases de données (Scopus, WoS...)
- Fichier utiles pour la normalisation des affiliations (fichiers utilisés par défaut disponibles)
    - Fichier donnant des informations sur la structure des adresses pour chaque pays (ie. code postal)
    - Fichier donnant la liste des villes par pays
    - Fichier donnant le nom normalisé et les dénominations pouvant être rencontrées dans les métadonnées des publications pour chaque affiliation par pays
    - Fichier donnant le nom normalisé et les dénominations pouvant être rencontrées dans les métadonnées des publications pour l'affiliation de l'utilisateur
    - Fichier donnant les différents types d'affiliation et leur ordre de priorité

Output Data
===========

- Les données de sortie sont des données décrites dans le descriptif des fonctions.
- Néanmoins, le module de démonstration propose des fonctions premettant la sauvegarde de ces données dans des formats de fichier compatibles avec un traitement par les outils de bureautique disponibles de manière standard chez les utilisateurs (XLSX, TXT...)**
