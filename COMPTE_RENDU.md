# SPARK - Partionnement d'observations dans le ciel 

>Date : Nov. 2017 

>Auteurs : Alice MONTEL et Mathilde Boltenhagen

>Langage : Python 3.6.3

>Outils : pyspark, pytest  

# Utilisation 

# Démarche

Afin de réaliser le partitionnement des sources, nous avons choisi de créer une classe **MapOfBlocks** qui permet de stocker toutes les informations relatives à la grille de la voûte céleste que l'on veut imaginer.
La première étape sera donc d'initialiser cette classe.
Pour cela, on va d'abord diviser la taille de nos données sources par la taille de blocs souhaitée, ici 128Mo, afin de savoir combien de blocs devraient être créés s'ils faisaient tous 128Mo.
On décompose ensuite ce nombre de blocs en un produit de facteurs premiers. Ainsi, on connaît la configuration de notre grille représentant la voûte céleste, c'est-à-dire, le nombre de lignes et de colonnes.
On crée ensuite un RDD qui nous permet de récupérer les valeurs minimales et maximales pour chacune des variables ra et decl. On obtient donc les coordonnées de notre grille. Comme on sait combien de lignes et de colonnes on souhaite, il suffit de diviser l'intervalle de coordonnées par le nombre de case souhaité, soit :
>(max_ra - min_ra)/facteur1 = i et (max_decl - min_decl)/facteur2 = j,

où facteur1*facteur2 = nombre de blocs et i (respectivement j) est donc le pas pour chaque colonne (respectivement ligne).
Grâce à ces pas, on peut définir les coordonnées minimales et maximales pour chacune de nos partitions.On crée donc un dictionnaire python qui regroupe les blocs et leurs coordonnées, de la forme :
>(1 : (min_ra,max_ra),(min_decl, max_decl), 2: (min_ra, max_ra), (min_decl,max_decl) [....])

Ensuite, pour chaque source, on récupère les coordonnées ra et decl et on cherche le bloc dans lequel elle devrait se trouver. Cela permet de compter le nombre de source qui devrait se trouver dans chaque bloc, et donc de compter le nombre de ligne par partition. 
Ensuite, on répartit les sources et on les écrit dans des fichiers distincts correspondants aux blocs voulus. 


# Production 

# Test en Local avec pystest 
