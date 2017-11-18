import pytest
from src.main import V1
from src.main import MapOfBlocks

pytestmark = pytest.mark.usefixtures("spark_context")

def test_get_block_number_with_margins_v2(spark_context):
    # get the name of the source path (file or directory)
    sourceDirectory = "../../Source/source-sample.csv"

    # initialize a MapOfBlocks object
    mapOfBlocks = MapOfBlocks.MapOfBlocks(5000, 128)

    # set min_ra, max_ra, min_decl, max_decl, step_ra, step_decl and create dict
    mapOfBlocks.setCoordinates(V1.getCoordinatesMinMax(sourceDirectory, spark_context))

    # first version
    mapOfBlocks.create_dict_coord()
    
    assert mapOfBlocks.nbBlocks==40

    assert mapOfBlocks.max_ra==358.0904167299142
    assert mapOfBlocks.min_ra==357.9542095807038
    assert mapOfBlocks.max_decl==3.1752713951351934
    assert mapOfBlocks.min_decl==2.5646291352701804

    #get number of lines per blocks
    nbLinesPerBlocks = V1.getNbLinePerPatition_V1(sourceDirectory, spark_context, mapOfBlocks)


    assert nbLinesPerBlocks== [(38,4),(36,1),(18,3),(0,11),(10,6),(37,3),(39,2),(29,10),(27,3),(19,10),(31,1),(21,1),(5,1)]

    '''
    to get the tests files and properties files, please enable those comments 
    '''
    #V1.partitioning_V1(sourceDirectory, "../../Source/test", spark_context, mapOfBlocks)
    #MapOfBlocks.writeNbLinesInPropertiesFile("../../Source", nbLinesPerBlocks, mapOfBlocks, spark_context)


