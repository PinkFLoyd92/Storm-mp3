from fileReader import MetricsReader
import easygui
import os.path
from sys import argv

def main():

    filename = easygui.fileopenbox()
    filename = os.path.basename(filename)
    metricsReader = MetricsReader(os.path.dirname(os.getcwd()) +'/'+ (filename))
    metricsReader.fill_dictionary()
    print(metricsReader.metrics_lines[0])
    # for line in metricsReader.metrics_lines:
        # print (line)
    
        
main()