# SLR207

> Rapport du projet SLR207 à Télécom Paris - **Tristan Perrot**

## Étape 1

J'ai donc implémenter un logiciel en java qui compte le nombre d’occurrences des mots d’un fichier d’entrée de manière non parallélisée. La structure de donnée la plus adaptée est l'**HashMap** car cela permet d'enregistrer des clefs associées à des valeurs (ici les mots associés à leur occurrence).

* Lors du test sur **le code forestier de Mayotte** mon code a fonctionné du premier coup.
* En ce qui concerne **le code de la déontologie de la police nationale**, les 5 premiers mots qui ressemblent à des mots (hors articles, etc..) sont :
    1. police
    2. article
    3. nationale
    4. titre
    5. fonctionnaire
* En ce qui concerne **le code du domaine public fluvial**, les 5 premiers mots qui ressemblent à des mots (hors articles, etc..) sont :
    1. article
    2. bateau
    3. tribunal
    4. bureau
    5. navigation
* En ce qui concerne **le code de la santé publique**, les 5 premiers mots qui ressemblent à des mots (hors articles, etc..) sont :
    1. article
    2. santé
    3. sont
    4. directeur
    5. conditions
