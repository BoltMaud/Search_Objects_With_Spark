import pytest

from src.main import V4
from src.main import MapOfBlocks

pytestmark = pytest.mark.usefixtures("spark_context")

def test_get_block_number_with_margins_v2(spark_context):
    # get the name of the source path (file or directory)
    sourceDirectory = "../../Source/source-sample.csv"

    # initialize a MapOfBlocks object
    mapOfBlocks = MapOfBlocks.MapOfBlocks(5000, 128)

    # set min_ra, max_ra, min_decl, max_decl, step_ra, step_decl and create dict
    mapOfBlocks.setCoordinates(V4.getCoordinatesMinMax_bis(sourceDirectory, spark_context))

    # create the dict of coordinates
    mapOfBlocks.create_dict_coord()

    #get number of lines to find which block has too many lines
    nbLinesPerBlocks = V4.getNbLinePerPatition_V3(sourceDirectory, spark_context, mapOfBlocks)

    #get the block that have 0 line to use them again
    mapOfBlocks.listOfEmptyBlocks= V4.getListOfEmptyBlocks(nbLinesPerBlocks,mapOfBlocks.nbBlocks)

    #delete coordinates if the block is empty
    mapOfBlocks.deleteCoordIfEmpty()

    #divide blocks if too many lines
    mapOfBlocks.divideBlocks(nbLinesPerBlocks,2)

    #get the new number of lines per block
    dictNbLinesPerBlocks_ = V4.getNbLinePerPatition_V3(sourceDirectory, spark_context, mapOfBlocks)


    '''
    to get the tests files and properties files, please enable those comments 
    '''
    #do the partitions
    #V4.partitioning_V3(sourceDirectory,"../../Source/test",spark_context,mapOfBlocks)

    #MapOfBlocks.writeNbLinesInPropertiesFile("../../Source", dictNbLinesPerBlocks_, mapOfBlocks, spark_context)


