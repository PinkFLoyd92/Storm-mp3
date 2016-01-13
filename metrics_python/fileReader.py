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
            if any(s in line for s in ("metric","Metric")):
                self.metrics_lines.append(line)
                print(line)
                new_file.write(line)
        print("Loaded metrics file")

    
