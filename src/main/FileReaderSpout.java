package main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.jboss.resteasy.client.jaxrs.ResteasyClient;
import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;
import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;

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
                                       + conf.get("data.txt"));
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
        /*if (completed) {
            try {
                //Thread.sleep(1000);
                this._collector.emit(new Values(" "));
            } catch (Exception e) {

            }
            return ;
        }*/
        //Client cliente;
        String str;
        ResteasyClient client = new ResteasyClientBuilder().build();
        ResteasyWebTarget target = client.target("http://192.168.1.108:8000/");
        Response response = target.request().get();
        str = response.readEntity(String.class);
        //cliente = ClientBuilder.newClient();
        //target = cliente.target("http://192.168.1.108:8000/");
        while(true)
        {
            try {
                str = target.request(MediaType.TEXT_PLAIN).get(String.class);
                this._collector.emit(new Values(str), str);
                Utils.sleep(500);
            }catch (Exception e){
                System.out.println("Error consuming webservice...");
            }
        }

        //Utils.sleep(100);
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
