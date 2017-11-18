# Date : Nov. 2017
# Auteur : Montel Alice, Boltenhagen Mathilde
##########################################################################################
####                     Version with simple partitions                             ######
##########################################################################################

from pyspark import SparkContext

#from src.main import MapOfBlocks
import sys
#comment to test
import MapOfBlocks


'''
get the min and max of ra and decl from a csv file or a directory
@:param dir (string) : path
'''
def getCoordinatesMinMax(dir,sc):
    #if there is csv written in the path, then it's only one file
    if "csv" in dir :
        rdd=sc.textFile(dir)
    #else we should work with all the files
    else :
        rdd = sc.textFile(dir+"/*.csv")

    # the ra and decl are line[6] and line[9]
    result =  rdd.filter(lambda line: len(line) > 0)\
        .map(lambda line: line.split(','))\
        .map(lambda line :( 1, [float(line[6]),float(line[6]),float(line[9]),float(line[9])]))\
        .reduceByKey(lambda x,y: [min(x[0],y[0]),max(x[1],y[1]),min(x[2],y[2]),max(x[3],y[3])] ).collect()
    return result[0][1]


'''
do the partitioning using MapOfBlocks class WITHOUT MARGIN
'''
def partitioning_V1(dir,dir_result,sc,dict):
    # if there is csv written in the path, then it's only one file
    if "csv" in dir:
        rdd=sc.textFile(dir)
    else :
        rdd = sc.textFile(dir+"/*.csv")

    rdd.filter(lambda line: len(line) > 0)\
    .map(lambda line : [ line, line.split(",")[6] , line.split(",")[9] ] )\
    .map(lambda x : ( dict.get_block_number_without_margin( float(x[1]) , float(x[2]) ) , x[0] ) )\
    .partitionBy(dict.nbBlocks)\
    .saveAsHadoopFile(dir_result, "org.apache.hadoop.mapred.TextOutputFormat" )

'''
get the number of each ra/decl per partition WITHOUT MARGIN
return dict { 1 : [ ..] .. }
'''

def getNbLinePerPatition_V1(dir, sc, dict):
    # if there is csv written in the path, then it's only one file
    if "csv" in dir:
        rdd = sc.textFile(dir)
    else:
        rdd = sc.textFile(dir + "/*.csv")

    tab = rdd.filter(lambda line: len(line) > 0) \
        .map(lambda line: [line.split(",")[6], line.split(",")[9]]) \
        .map(lambda x: (dict.get_block_number_without_margin(float(x[0]), float(x[1])), 1)) \
        .reduceByKey(lambda x, y: x + y).collect()
    return tab


def main(TOTALSIZE, SIZEOFBLOCK):
    #get the name of the source path (file or directory)
    sourceDirectory = sys.argv[1]
    sc = SparkContext("local", "App")

    #initialize a MapOfBlocks object
    mapOfBlocks = MapOfBlocks.MapOfBlocks(TOTALSIZE, SIZEOFBLOCK)

    # set min_ra, max_ra, min_decl, max_decl, step_ra, step_decl and create dict
    mapOfBlocks.setCoordinates(getCoordinatesMinMax(sourceDirectory,sc))

    mapOfBlocks.create_dict_coord()

    #get the name of the new directory
    resultDirectory= sys.argv[2]

    #get the nb of lines without divided
    nbLinesPerBlocks= getNbLinePerPatition_V1(sourceDirectory, sc, mapOfBlocks)

    #do the partition
    partitioning_V1(sourceDirectory, resultDirectory, sc, mapOfBlocks)

    # write the properties in a file
    MapOfBlocks.writeNbLinesInPropertiesFile(resultDirectory, nbLinesPerBlocks, mapOfBlocks, sc)

#comment for the tests
main(5000,128)