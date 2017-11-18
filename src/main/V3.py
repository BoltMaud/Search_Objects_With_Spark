# Date : Nov. 2017
# Auteur : Montel Alice, Boltenhagen Mathilde
##########################################################################################
####                  Version with simple partitins and ecliptic coordinates        ######
##########################################################################################

import sys
from pyspark import SparkContext
import sys

#comment to test
#from src.main import MapOfBlocks
#from src.main import mymath
import MapOfBlocks
import mymath


'''
get min and max of lambda and B
'''
def getCoordinatesMinMax_bis(dir,sc):
    #if there is csv written in the path, then it's only one file
    if "csv" in dir :
        rdd=sc.textFile(dir)
    #else we should work with all the files
    else :
        rdd = sc.textFile(dir+"/*.csv")

    # the ra and decl are line[6] and line[9]
    result =  rdd.filter(lambda line: len(line) > 0) \
        .map(lambda line: line.split(',')) \
        .map(lambda line :( 1, [mymath.getL(float(line[6]), float(line[9])), mymath.getL(float(line[6]), float(line[9])) \
        , mymath.getB(float(line[6]), float(line[9])), mymath.getB(float(line[6]), float(line[9]))])) \
        .reduceByKey(lambda x,y: [min(x[0],y[0]),max(x[1],y[1]),min(x[2],y[2]),max(x[3],y[3])] ).collect()
    return result[0][1]


'''
get the number of each ra/decl per partition WITH MARGINS
return dict { 1 : [ ..] .. }
'''
def getNbLinePerPatition_V3(dir,sc,dict):
    # if there is csv written in the path, then it's only one file
    if "csv" in dir:
        rdd = sc.textFile(dir)
    else:
        rdd = sc.textFile(dir + "/*.csv")

    tab=rdd.filter(lambda line: len(line) > 0) \
        .map(lambda line: [ line.split(",")[6], line.split(",")[9]]) \
        .map(lambda x: dict.get_block_number_with_margins(mymath.getL(float(x[0]), float(x[1])),
                                                          mymath.getB(float(x[0]), float(x[1])), 1)) \
        .flatMap(lambda x: x.split("_")) \
        .map(lambda x: x.split(':')) \
        .map(lambda x : (int(x[0]),1))\
        .sortByKey()\
        .reduceByKey(lambda x,y : x+y).collect()
    return tab

'''
do the partitioning using MapOfBlocks class WITH MARGINS
'''
def partitioning_V3(dir,dir_result,sc,dict):
    # if there is csv written in the path, then it's only one file
    if "csv" in dir:
        rdd=sc.textFile(dir)
    else :
        rdd = sc.textFile(dir+"/*.csv")

    return rdd.filter(lambda line: len(line) > 0)\
    .map(lambda line : [ line, line.split(",")[6] , line.split(",")[9] ] )\
    .map(lambda x :  dict.get_block_number_with_margins(mymath.getL(float(x[1]), float(x[2])),
                                                        mymath.getB(float(x[1]), float(x[2])), x[0]))\
    .flatMap(lambda x : x.split("_") )\
    .map(lambda x : x.split(':')) \
        .map(lambda x: (int(x[0]), x[1])) \
        .partitionBy(dict.nbBlocks) \
        .saveAsHadoopFile(dir_result, "org.apache.hadoop.mapred.TextOutputFormat" )


def main(TOTALSIZE, SIZEOFBLOCK):
    #get the name of the source path (file or directory)
    sourceDirectory = sys.argv[1]
    sc = SparkContext("local", "App")

    #initialize a MapOfBlocks object
    mapOfBlocks = MapOfBlocks.MapOfBlocks(TOTALSIZE, SIZEOFBLOCK)

    # set min_ra, max_ra, min_decl, max_decl, step_ra, step_decl and create dict
    mapOfBlocks.setCoordinates(getCoordinatesMinMax_bis(sourceDirectory,sc))

    mapOfBlocks.create_dict_coord()

    #get the name of the new directory
    resultDirectory= sys.argv[2]

    #get the nb of lines without divided
    nbLinesPerBlocks= getNbLinePerPatition_V3(sourceDirectory, sc, mapOfBlocks)

    #do the partition
    partitioning_V3(sourceDirectory, resultDirectory, sc, mapOfBlocks)

    # write the properties in a file
    MapOfBlocks.writeNbLinesInPropertiesFile(resultDirectory, nbLinesPerBlocks, mapOfBlocks, sc)

#comment for test
main(5000,128)