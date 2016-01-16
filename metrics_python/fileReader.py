from sys import argv

class MetricsReader:
    file_name = ""
    metrics_lines = []
    diccionario_metricas = {}
    def __init__(self, file_name):
        "MetricsReader...."
        self.file_name = file_name
        self.getMetricsLines()

    def getMetricsLines(self):
        new_file = open("metrics_lines","w")
        for line in (open(self.file_name,'r')):
            # if any(s in line for s in ("metrics","__metricsbacktype.storm.metric.LoggingMetricsConsumer] INFO  b.s.m.LoggingMetricsConsumer - 1452635094	      localhost:1027")):
            if("1452635094	      localhost:1027" in line):
                line = self.preprocessLine(line)
                self.metrics_lines.append(line)
                new_file.write(line)
        print("Loaded metrics file. Please open the file metrics_lines to check it")

    def preprocessLine(self,line):
        length_localhost = len("localhost:1027")
        index_localHost = line.find("localhost:1027")
        line = line[index_localHost + length_localhost:]
        for x in line:
            if x.isdigit():
                numb_= line.find(x)
                line = line[numb_:]
                break
        return line

    def fill_dictionary(self):
        for line in self.metrics_lines:
            line = self.getTitle(line)

            
    def getTitle(self,line):
        for x in line:
            if(not x.isdigit() or not x.isalpha()):
                line = line[:line.find(x)]
                return line
        return line

