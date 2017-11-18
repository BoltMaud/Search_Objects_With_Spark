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
Grâce à ces pas, on peut définir les coordonnées minimales et maximales pour chacune de nos partitions. 
On crée donc un dictionnaire python qui regroupe les blocs et leurs coordonnées, de la forme :
>(1 : (min_ra,max_ra),(min_decl, max_decl), 2: (min_ra, max_ra), (min_decl,max_decl) [....])

Ensuite, pour chaque source, on récupère les coordonnées ra et decl et on cherche le bloc dans lequel elle devrait se trouver. Cela permet de compter le nombre de source qui devrait se trouver dans chaque bloc, et donc de compter le nombre de ligne par partition. 
Ensuite, on répartit les sources et on les écrit dans des fichiers distincts correspondants aux blocs voulus. 
On crée donc un fichier par partition sur HDFS, dans un répertoire précisé en paramètre lors du lancement de l'application, comme suit : 
> spark-submit monapp.py /chemin/vers/sources /chemin/vers/resultats

A la fin de l'exécution, le répertoire resultats contiendra une série de fichiers csv, nommé `part-00000`, `part-00001`, etc., contenant les lignes de sources de la partition adéquate. 


# Production 

### Première approximation
Une première version permet de répartir les sources dans des cases de **dimensions fixes**. Cela a pour effet de créer des partitions très lourdes (avec beaucoup de sources), et d'autres quasiment vides, puisque les sources sont très inégalement réparties sur la grille. 
On observe facilement l'inaglité de répartition sur les histogrammes suivants : 
[HISTOGRAMS_V1]

### Deuxième approche 
Dans cette deuxième approche, on considère un certain **recoupement** (ici 5%) entre les blocs. Cela implique que les fichiers csv résultants du partitionnement contiennent plus de lignes. Cependant, on doit avoir le même nombre de partitions que dans la première approche. 
On constate que c'est bien ce qu'on obtient grâce aux histogrammes ci-dessous : 
[HISTOGRAMS_V2]

### Troisième approche
Jusqu'à présent, on utilisait les coordonnées célestes (`ra` et `decl`) pour répartir les sources. Afin d'avoir une meilleure approximation, on utilise ra et decl pour calculer les coordonnées écliptiques `lambda` et `beta`. 


# Test en Local avec pystest 
