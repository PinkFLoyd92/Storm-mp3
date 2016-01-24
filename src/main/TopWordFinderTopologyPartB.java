package main;

import backtype.storm.Config;
//import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.metric.SystemBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology reads a file and counts the words in that file
 */
public class TopWordFinderTopologyPartB {

  public static void main(String[] args) throws Exception {


        TopologyBuilder builder = new TopologyBuilder();

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(3);
        //In this example, we are registering the metrics consumer with a
        // parallelism hint of 2. Here is the line we need to add when defining the topology.
        config.registerMetricsConsumer(LoggingMetricsConsumer.class, 5);

        config.put("data",args[1]);
        // LocalCluster cluster = new LocalCluster();
        /*
        ----------------------TODO-----------------------
        Task: wire up the topology

        NOTE:make sure when connecting components together, using the functions setBolt(name,…) and setSpout(name,…),
        you use the following names for each component:
        java.FileReaderSpout -> "spout"
        java.SplitSentenceBolt -> "split"
        java.WordCountBolt -> "count"
        ------------------------------------------------- */

        builder.setSpout("spout",new FileReaderSpout(),2);
        builder.setBolt("split",new SplitSentenceBolt(),8).shuffleGrouping("spout");
        builder.setBolt("count",new WordCountBolt(),12).fieldsGrouping("split",new Fields("word"));
        config.setMaxTaskParallelism(5);

        try {
            // cluster.submitTopology(args[0], config, builder.createTopology());
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }catch (AlreadyAliveException alreadyAliveException){
            System.out.println(alreadyAliveException);
        }catch (InvalidTopologyException invalidTopologyException){
            System.out.println(invalidTopologyException);
        }

        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("word-count", config, builder.createTopology());

        //wait for 2 minutes and then kill the job
        //Thread.sleep( 2 * 60 * 1000);

        //cluster.shutdown();
  }
}
