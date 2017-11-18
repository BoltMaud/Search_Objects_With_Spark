import pytest
from . import  TP2_spark
from . import MapOfBlocks

pytestmark = pytest.mark.usefixtures("spark_context")

def test_get_block_number_with_margins_v2(spark_context):
    # get the name of the source path (file or directory)
    sourceDirectory = "./Source/source-sample.csv"

    # initialize a MapOfBlocks object
    mapOfBlocks = MapOfBlocks.MapOfBlocks(5000, 128)

    # set min_ra, max_ra, min_decl, max_decl, step_ra, step_decl and create dict
    mapOfBlocks.setCoordinates(TP2_spark.getCoordinatesMinMax_bis(sourceDirectory, spark_context))

    # first version
    mapOfBlocks.create_dict_coord()


    nbLinesPerBlocks = TP2_spark.getNbLinePerPatition_V2_bis(sourceDirectory, spark_context, mapOfBlocks)
    TP2_spark.partitioning_V2_bis(sourceDirectory,"./Source/test",spark_context,mapOfBlocks)
    #mapOfBlocks.listOfEmptyBlocks= TP2_spark.getListOfEmptyBlocks(nbLinesPerBlocks,mapOfBlocks.nbBlocks)
    #mapOfBlocks.deleteCoordIfEmpty()
    #mapOfBlocks.divideBlocks(nbLinesPerBlocks,1)
    #TP2_spark.partitioning_V2(sourceDirectory,"./Source/test",spark_context,mapOfBlocks)

    #dictNbLinesPerBlocks_ = TP2_spark.getNbLinePerPatition_V2(sourceDirectory, spark_context, mapOfBlocks)
    #print("---ffffffffffff")
    TP2_spark.writeNbLinesInPropertiesFile("./Source", nbLinesPerBlocks,mapOfBlocks,spark_context)


