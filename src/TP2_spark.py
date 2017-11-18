from pyspark import SparkContext
import sys

from . import mymath
from . import MapOfBlocks
import matplotlib.pyplot as plt
import numpy

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
        .map(lambda line :( 1, [mymath.getL(float(line[6]), float(line[9])),mymath.getL(float(line[6]), float(line[9])) \
        ,mymath.getB(float(line[6]), float(line[9])),mymath.getB(float(line[6]), float(line[9]))])) \
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
def getNbLinePerPatition_V1(dir,sc,dict):
    # if there is csv written in the path, then it's only one file
    if "csv" in dir:
        rdd = sc.textFile(dir)
    else:
        rdd = sc.textFile(dir + "/*.csv")

    tab=rdd.filter(lambda line: len(line) > 0) \
        .map(lambda line: [ line.split(",")[6], line.split(",")[9]]) \
        .map(lambda x: (dict.get_block_number_without_margin(float(x[0]), float(x[1])),1)) \
        .reduceByKey(lambda x,y : x+y).collect()
    return tab

'''
get the number of each ra/decl per partition WITH MARGINS
return dict { 1 : [ ..] .. }
'''
def getNbLinePerPatition_V2_bis(dir,sc,dict):
    # if there is csv written in the path, then it's only one file
    if "csv" in dir:
        rdd = sc.textFile(dir)
    else:
        rdd = sc.textFile(dir + "/*.csv")

    tab=rdd.filter(lambda line: len(line) > 0) \
        .map(lambda line: [ line.split(",")[6], line.split(",")[9]]) \
        .map(lambda x: dict.get_block_number_with_margins(mymath.getL(float(x[0]), float(x[1])),mymath.getB(float(x[0]),float(x[1])),1)) \
        .flatMap(lambda x: x.split("_")) \
        .map(lambda x: x.split(':')) \
        .map(lambda x : (int(x[0]),1))\
        .sortByKey()\
        .reduceByKey(lambda x,y : x+y).collect()
    return tab

'''
get the number of each ra/decl per partition WITH MARGINS
return dict { 1 : [ ..] .. }
'''
def getNbLinePerPatition_V2(dir,sc,dict):
    # if there is csv written in the path, then it's only one file
    if "csv" in dir:
        rdd = sc.textFile(dir)
    else:
        rdd = sc.textFile(dir + "/*.csv")

    tab=rdd.filter(lambda line: len(line) > 0) \
        .map(lambda line: [ line.split(",")[6], line.split(",")[9]]) \
        .map(lambda x: dict.get_block_number_with_margins( float(x[0]), float(x[1]),1)) \
        .flatMap(lambda x: x.split("_")) \
        .map(lambda x: x.split(':')) \
        .map(lambda x : (int(x[0]),1))\
        .reduceByKey(lambda x,y : x+y).collect()
    return tab

'''
do the partitioning using MapOfBlocks class WITH MARGINS
'''
def partitioning_V2(dir,dir_result,sc,dict):
    # if there is csv written in the path, then it's only one file
    if "csv" in dir:
        rdd=sc.textFile(dir)
    else :
        rdd = sc.textFile(dir+"/*.csv")

    return rdd.filter(lambda line: len(line) > 0)\
    .map(lambda line : [ line, line.split(",")[6] , line.split(",")[9] ] )\
    .map(lambda x :  dict.get_block_number_with_margins( float(x[1]) , float(x[2]) ,x[0] )   )\
    .flatMap(lambda x : x.split("_") )\
    .map(lambda x : x.split(':')) \
        .map(lambda x: (int(x[0]), x[1])) \
        .partitionBy(dict.nbBlocks) \
        .saveAsHadoopFile(dir_result, "org.apache.hadoop.mapred.TextOutputFormat" )
'''
do the partitioning using MapOfBlocks class WITH MARGINS
'''
def partitioning_V2_bis(dir,dir_result,sc,dict):
    # if there is csv written in the path, then it's only one file
    if "csv" in dir:
        rdd=sc.textFile(dir)
    else :
        rdd = sc.textFile(dir+"/*.csv")

    return rdd.filter(lambda line: len(line) > 0)\
    .map(lambda line : [ line, line.split(",")[6] , line.split(",")[9] ] )\
    .map(lambda x :  dict.get_block_number_with_margins(mymath.getL(float(x[1]), float(x[2])),mymath.getB(float(x[1]),float(x[2])),x[0] )   )\
    .flatMap(lambda x : x.split("_") )\
    .map(lambda x : x.split(':')) \
        .map(lambda x: (int(x[0]), x[1])) \
        .partitionBy(dict.nbBlocks) \
        .saveAsHadoopFile(dir_result, "org.apache.hadoop.mapred.TextOutputFormat" )

'''
write a file with the properties 
'''
def writeNbLinesInPropertiesFile(resultDirectory,nbLinesPerBlocks,mapOfBlocks,sc):
    text=mapOfBlocks.getPropertiesAsString()
    text+="block number,nbLines\n"
    for block in nbLinesPerBlocks:
       text+=str(block[0])+","+str(block[1])+"\n"
    sc.parallelize((text,1)).saveAsTextFile(resultDirectory+"/properties")


'''
get list of the blocks that have 0 line
'''
def getListOfEmptyBlocks(nbLinesPerBlocks,nbBlocks):
    list =[]
    listOfNotEmptyBlocks=[]
    print(nbLinesPerBlocks)
    #get the list of the blocks that have lines
    for block in nbLinesPerBlocks:
        listOfNotEmptyBlocks.append(int(block[0]))
    # create list of available blocks
    for i in range (0,nbBlocks):
        if i not in listOfNotEmptyBlocks:
            list.append(i)
    return list

def createHist(nbLinesPerBlocks,directory):
    x=[]
    height=[]
    for block in nbLinesPerBlocks :
        x.append(int(block[0]))
        height.append(block[1])
    width=1.0
    fig=plt.figure()
    plt.bar(x,height,width,color='b')
    plt.savefig(directory+"/hist.png")
    plt.show()

def main_test(TOTALSIZE, SIZEOFBLOCK):
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
    writeNbLinesInPropertiesFile(resultDirectory, nbLinesPerBlocks, mapOfBlocks, sc)


# ------------------------------------------------------------------------------------------
def main(TOTALSIZE, SIZEOFBLOCK,NBLINESPERBLOCK):

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
    nbLinesPerBlocks= getNbLinePerPatition_V2(sourceDirectory, sc, mapOfBlocks)

    #get the blocks that are empty to use them when divided
    mapOfBlocks.listOfEmptyBlocks = getListOfEmptyBlocks(nbLinesPerBlocks, mapOfBlocks.nbBlocks)

    #delete the coords of the blocks that are empty
    mapOfBlocks.deleteCoordIfEmpty()

    #create new blocks with id of the deleted blocks
    # create blocks only if size of block > nbmax
    mapOfBlocks.divideBlocks(nbLinesPerBlocks, NBLINESPERBLOCK)

    #do the partition
    partitioning_V2(sourceDirectory, resultDirectory, sc, mapOfBlocks)

    #get the new number of lines
    dictNbLinesPerBlocks_ = getNbLinePerPatition_V2(sourceDirectory, sc, mapOfBlocks)

    # write the properties in a file
    writeNbLinesInPropertiesFile(resultDirectory, dictNbLinesPerBlocks_, mapOfBlocks, sc)




