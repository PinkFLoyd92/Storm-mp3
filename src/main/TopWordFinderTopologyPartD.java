package main;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology reads a file and counts the words in that file, then finds the top N words.
 */
public class TopWordFinderTopologyPartD {

  private static final int N = 10;

  public static void main(String[] args) throws Exception {


    TopologyBuilder builder = new TopologyBuilder();

    Config config = new Config();
    config.setDebug(true);


    /*
    ----------------------TODO-----------------------
    Task: wire up the topology

    NOTE:make sure when connecting components together, using the functions setBolt(name,…) and setSpout(name,…),
    you use the following names for each component:
    
    java.FileReaderSpout -> "spout"
    java.SplitSentenceBolt -> "split"
    java.WordCountBolt -> "count"
	java.NormalizerBolt -> "normalize"
    java.TopNFinderBolt -> "top-n"


    ------------------------------------------------- */
    builder.setSpout("spout",new FileReaderSpout(),5);
    builder.setBolt("split",new SplitSentenceBolt(),8).shuffleGrouping("spout");
    builder.setBolt("count",new WordCountBolt(),12).fieldsGrouping("split",new Fields("word"));
    builder.setBolt("normalize",new NormalizerBolt(),8);
    builder.setBolt("top-n",new TopNFinderBolt(10),12);
    config.setMaxTaskParallelism(3);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", config, builder.createTopology());

    //wait for 2 minutes and then kill the job
    Thread.sleep(2 * 60 * 1000);

    cluster.shutdown();
  }
}
