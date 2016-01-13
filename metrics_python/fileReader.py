from sys import argv

class MetricsReader:
    file_name = ""
    metrics_lines = []
    def __init__(self, file_name):
        "MetricsReader...."
        self.file_name = file_name
        self.getMetricsLines()

    def getMetricsLines(self):
        new_file = open("metrics_lines","w")
        for line in (open(self.file_name,'r')):
            # if any(s in line for s in ("metrics","__metricsbacktype.storm.metric.LoggingMetricsConsumer] INFO  b.s.m.LoggingMetricsConsumer - 1452635094	      localhost:1027")):
            if("1452635094	      localhost:1027" in line):
                self.metrics_lines.append(line)
                new_file.write(self.preprocessLine(line))
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
