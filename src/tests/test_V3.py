import pytest

from src.main import V3
from src.main import MapOfBlocks

pytestmark = pytest.mark.usefixtures("spark_context")

def test_get_block_number_with_margins_v2(spark_context):
    # get the name of the source path (file or directory)
    sourceDirectory = "../../Source/source-sample.csv"

    # initialize a MapOfBlocks object
    mapOfBlocks = MapOfBlocks.MapOfBlocks(5000, 128)

    # set min_ra, max_ra, min_decl, max_decl, step_ra, step_decl and create dict
    mapOfBlocks.setCoordinates(V3.getCoordinatesMinMax_bis(sourceDirectory, spark_context))

    # first version
    mapOfBlocks.create_dict_coord()

    assert mapOfBlocks.nbBlocks==40

    assert mapOfBlocks.max_ra==0.5920175714227844
    assert mapOfBlocks.min_ra==-0.02049581740538999
    assert mapOfBlocks.max_decl==0.13407940259689952
    assert mapOfBlocks.min_decl==-0.0021299325969507442


    nbLinesPerBlocks = V3.getNbLinePerPatition_V3(sourceDirectory, spark_context, mapOfBlocks)

    assert nbLinesPerBlocks == [(2, 1),(4, 2),(6, 5),( 8, 10),(16, 1),(30, 1),( 34, 6),( 36, 9),(3, 6),(5, 1),(7, 5),\
                               ( 9, 9),(13, 3),( 19, 1),( 33, 1),( 35, 1),(37, 1)]

    '''
    to get the tests files and properties files, please enable those comments 
    '''
    #V3.partitioning_V3(sourceDirectory, "../../Source/test", spark_context, mapOfBlocks)
    #MapOfBlocks.writeNbLinesInPropertiesFile("../../Source", nbLinesPerBlocks, mapOfBlocks, spark_context)


