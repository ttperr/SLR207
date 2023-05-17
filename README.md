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
* Pour l'extrait de page internet, j'ai obtenu ces données :
  * Durée de lecture et de séparation : 4.039s
  * Durée de comptage : 5.271s
  * Durée de tri et d'affichage des 50 premiers mots : 6.809s

## Étape 2

Pour l'étape 2, j'ai utilisé l'ordinateur **tp-1a201-12** (tp-1a201-12.enst.fr). 
Pour trouver son nom à l'aide d'une commande il faut écrire :

```bash
hostname -f
```

> le -f permet d'avoir le nom complet (avec le nom de domaine).

Sur les ordinateurs de l'école je crois que cela est écrit un peu partout.

Pour trouver les addresses IP des ordinateurs de l'école, il faut utiliser la commande :

```bash
ifconfig
```

Pour obtenir les adresses IP en ligne de commande à partir du nom d'un ordinateur il faut utiliser la commande :

```bash
host <nom de l'ordinateur>
```

Pour obtenir les noms associés en ligne de commande à partir d'une adresse IP il faut utiliser la commande :

```bash
host <adresse IP>
```

Les appels à la fonction ping avec comme argument le nom court, le nom long ou bien l'adresse IP fonctionnent très bien.

Pour lancer un calcul en ligne de commande, il est possible de faire comme cela:

```bash
echo $((2 + 3))
```

> Par ailleurs, cette commande permet d'avoir le résultat d'un calcul avec un seul appuie sur la touche entrée.

Pour lancer ce calcul à distance, nous pouvons procéder à l'aide de *ssh* comme cela :

```bash
ssh <nom de l'ordinateur> echo $((2 + 3))
```

> Par ailleurs, cette commande permet d'avoir le résultat d'un calcul immédiatement après saisie du mot de passe.

Pour ne plus avoir à faire à ce mot de passe il suffit d'utiliser d'enregistrer sa clé publique sur l'ordinateur distant. Pour cela il faut utiliser la commande :

```bash
ssh-copy-id <nom de l'ordinateur>
```

## Étape 3
