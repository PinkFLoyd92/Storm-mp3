package main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import javax.swing.JOptionPane;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FileReaderSpout implements IRichSpout {
    private SpoutOutputCollector _collector;
    private TopologyContext context;
    private boolean completed  = false;
    private FileReader fileReader;

    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        /*
          ----------------------TODO-----------------------
          Task: initialize the file reader
          ------------------------------------------------- */
        try {
            this.context = context;
            this.fileReader = new FileReader(conf.get("data").toString()); // ¿está bien eso?
            //this.fileReader = new FileReader("data.txt");
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file "
                                       + conf.get("data"));
        }

        this._collector = collector;
    }

    @Override
    public void nextTuple() {

        /*
          ----------------------TODO-----------------------
          Task:
          1. read the next line and emit a tuple for it
          2. don't forget to sleep when the file is entirely read to prevent a busy-loop

          ------------------------------------------------- */
	/* ProcessBuilder pb = new ProcessBuilder("python3","/home/sebas/mp3_python/main.py");
        try {
            Process p = pb.start();
        } catch (IOException e) {
            JOptionPane.showMessageDialog(null,e.getMessage(),"debug",JOptionPane.INFORMATION_MESSAGE);
	    }*/
        if (completed) {
            try {
                //Thread.sleep(1000);
                this._collector.emit(new Values(" "));

            } catch (Exception e) {

            }
            return ;
        }
        String str;
        BufferedReader reader = new BufferedReader(fileReader);
        try {
            while ((str = reader.readLine()) != null) {
                this._collector.emit(new Values(str), str);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            completed = true;
        }       //Utils.sleep(100);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word"));

    }

    @Override
    public void close() {
        /*
          ----------------------TODO-----------------------
          Task: close the file


          ------------------------------------------------- */
        try {
            this.fileReader.close();
        }catch(IOException e){
            System.out.println("error");
        }
    }


    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
