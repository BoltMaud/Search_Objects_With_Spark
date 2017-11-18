from src.main import mymath


class MapOfBlocks :

    # coordinates of the furthest points
    # intialized at 0
    min_ra, max_ra, min_decl, max_decl= (0,0,0,0)

    #number of blocks
    nbBlocks =0

    #number of lines and columns
    nbRows,nbCols=(0,0)

    #step for axis ra and axis decl
    step_ra, step_decl=(0,0)

    # all the coordinates of the blocks and the number of the block
    # key = coord ; value = number
    dictOfCoord ={}

    listOfEmptyBlocks=[]

    def __init__(self,size,sizeOfBlocks):
        #set the number of blocks with the size sizeOfBlocks
        self.compute_nb_blocks(size,sizeOfBlocks)


    def setCoordinates(self,coordinatesMinMax):
        # set the min_ra, max_ra, min_decl, max_decl
        self.min_ra, self.max_ra, self.min_decl, self.max_decl=coordinatesMinMax

        #get the number of lines and columns
        self.get_couple_rows_cols()


        #set the steps of ra and decl axis depending on the number of cols and rows
        self.set_steps()

    '''
    get the good number of blocks
    Contraints : should not be prime (because it's complicated to divide a map with a prime number
    If size cannot be divided by sizeOfBlocks, then add 1 block
    '''
    def compute_nb_blocks(self,size,sizeOfBlocks):
        # if the size is divided per 128
        if size % sizeOfBlocks == 0 and not mymath.is_prime(size//sizeOfBlocks):
            self.nbBlocks= size//sizeOfBlocks
        else :
            if  size % sizeOfBlocks != 0 and mymath.is_prime(size//sizeOfBlocks +1):
                self.nbBlocks=size//sizeOfBlocks +2
            else :
                self.nbBlocks=size//sizeOfBlocks +1

    '''
    complete nbRows and nbCols of the object using the prime factors
    '''
    def get_couple_rows_cols(self):
        divs = mymath.get_prime_factors(self.nbBlocks)
        list_fact1 = []
        list_fact2 = []
        #for each prime factors, one goes to the line, the next one to colunm
        #then the number of line and columns should be similar
        for i in range(0, len(divs)):
            if i % 2:
                list_fact1.append(divs[i])
            else:
                list_fact2.append(divs[i])
        fact1 = 1
        fact2 = 1
        for i in range(0, len(list_fact1)):
            fact1 *= list_fact1[i]
        for i in range(0, len(list_fact2)):
            fact2 *= list_fact2[i]

        self.nbRows=fact1
        self.nbCols=fact2


    '''
    set the steps of the axis 
    '''
    def set_steps(self):
        self.step_ra=(self.max_ra-self.min_ra)/self.nbCols
        self.step_decl=(self.max_decl-self.min_decl)/self.nbRows

    '''
    create the dict of the coordinates of the blocks with the number of the blocks
    '''
    def create_dict_coord(self):
        dict = {}
        num = 0
        for i in range(0,self.nbCols):
            for j in range (0,self.nbRows):
                max_decl=self.min_decl+ (j+1) * self.step_decl
                max_ra=self.min_ra+(i+1)* self.step_ra

                if(j==self.nbRows):
                    max_decl=self.max_decl
                if(i==self.nbCols):
                    max_ra=self.max_ra
                dict[num] = ((self.min_ra+i*self.step_ra,max_ra ), (self.min_decl+j*self.step_decl,max_decl))
                num += 1
        self.dictOfCoord=dict

    '''
    get the number of the block depending on a ra and a decl 
    version 1 : without margin
    '''
    def get_block_number_without_margin(self, ra, decl):
        for key in self.dictOfCoord:
            if ra >= self.dictOfCoord[key][0][0] and ra <= self.dictOfCoord[key][0][1] and decl >= self.dictOfCoord[key][1][0] and decl <= self.dictOfCoord[key][1][1]:
                return key
        # error
        return 0

    '''
    get the number of the blocks depending on a ra and a decl 
    version 2 : with margin
    '''
    def get_block_number_with_margins(self, ra, decl, line):
        # create margin depending on the axis
        margin_ra = ((self.max_ra - self.min_ra) / self.nbCols) * 0.05
        margin_decl = ((self.max_decl - self.min_decl) / self.nbRows) * 0.05

        string_of_blocks=""
        for key in self.dictOfCoord:
            if ra >= self.dictOfCoord[key][0][0]-margin_ra and ra <= self.dictOfCoord[key][0][1]+margin_ra and\
                            decl >= self.dictOfCoord[key][1][0]-margin_decl and decl <= self.dictOfCoord[key][1][1]+margin_decl:
                if string_of_blocks != "":
                    string_of_blocks+="_"
                string_of_blocks+= str(key)+":"+str(line)
        # error
        return string_of_blocks

    '''
    divide blocks that have to many lines 
    '''
    def divideBlocks(self,nbLinesPerBlocks,NBLINESMAX):
        #find the blocks that have to many lines
        for block in nbLinesPerBlocks:
            if block[1]>NBLINESMAX:
                # compute the number of new blocks needed
                nbNewBlocks=(block[1]//NBLINESMAX)
                #find the coord of the block to divide
                for key in self.dictOfCoord:
                    if int(key)==int(block[0]):
                        #set the size of one block
                        #only ra will be divided
                        sizeOfOneBlock_ra=(self.dictOfCoord[key][0][1]-self.dictOfCoord[key][0][0])/nbNewBlocks

                        #create the good number of subblocks
                        for i in range (0,nbNewBlocks):
                            #carefull for the last bordure
                            max_ra=self.dictOfCoord[key][0][0]+i*sizeOfOneBlock_ra+sizeOfOneBlock_ra
                            if(i==nbNewBlocks):
                                max_ra=self.dictOfCoord[key][0][1]
                            #create the coord using a part of the last coord
                            newCoord=((self.dictOfCoord[key][0][0]+i*sizeOfOneBlock_ra,max_ra), \
                                      (self.dictOfCoord[key][1][0] ,self.dictOfCoord[key][1][1]))

                            #if we used all the available id, create 10 new ids
                            if len(self.listOfEmptyBlocks)==0:
                                for i in range(self.nbBlocks,self.nbBlocks+10):
                                    self.listOfEmptyBlocks.append(i)
                                self.nbBlocks+=10
                            #create and add the new block to the map
                            newBlockNumber=self.listOfEmptyBlocks[0]
                            self.listOfEmptyBlocks.remove(newBlockNumber)
                            self.dictOfCoord[newBlockNumber]=newCoord

                        self.listOfEmptyBlocks.append(key)
                        self.dictOfCoord.pop(key)
                        #when a block is resolved, search for an new one
                        break

    '''
    delete the key:value of coords that aren't used
    '''
    def deleteCoordIfEmpty(self):
        listToDelete=[]
        for key in self.dictOfCoord:
            if key in self.listOfEmptyBlocks:
                listToDelete.append(key)
        for key in listToDelete:
            self.dictOfCoord.pop(key)




    '''
     write the properties of a MapOfBlocks in a file
     '''
    def getPropertiesAsString(self):
        text = "Size Of Map, " + str(self.nbBlocks) + "\n"
        text += "Number of rows," + str(self.nbRows) + "\n"
        text += "Number of Cols," + str(self.nbCols) + "\n"
        text += "Coordinates of Blocks : \n"
        text += "block number, min_ra,max_ra,min_decl,max_decl  \n"
        print(self.dictOfCoord)
        for key in self.dictOfCoord:
            text += str(key) + "," + str(self.dictOfCoord[key][0][0]) + "," + str(self.dictOfCoord[key][0][1]) \
                    + "," + str(self.dictOfCoord[key][1][0]) + "," + str(self.dictOfCoord[key][1][1]) + "\n"
        return text

# ----------------------------------------------------------------------------------------------
# End of class MapOfBlocks

'''
write a file with the properties 
'''
def writeNbLinesInPropertiesFile(resultDirectory,nbLinesPerBlocks,mapOfBlocks,sc):
    text=mapOfBlocks.getPropertiesAsString()
    text+="block number,nbLines\n"
    for block in nbLinesPerBlocks:
       text+=str(block[0])+","+str(block[1])+"\n"
    sc.parallelize((text,1)).saveAsTextFile(resultDirectory+"/properties")