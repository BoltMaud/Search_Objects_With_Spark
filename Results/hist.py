
import matplotlib.pyplot as plt
def createHist(file,name):
    file=open(file,"r")
    for line in file:
        if "nbLines" in line :
            break

    x = []
    height = []
    for line in file:
        if line !="\n":
            x.append(int(line.split(",")[0]))
            height.append(int(line.split(",")[1].split("\n")[0]))

    width = 1.0
    fig = plt.figure()
    plt.bar(x, height, width, color='b')
    plt.ylabel('number of sources')
    plt.xlabel('block numbers')
    plt.title(name)
    plt.savefig(name)
    plt.show()


createHist("result_prod_V3.csv","hist_prod_V3")