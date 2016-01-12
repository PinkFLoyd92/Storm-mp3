
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology reads a file and counts the words in that file
 */
public class TopWordFinderTopologyPartB {

  public static void main(String[] args) throws Exception {


    TopologyBuilder builder = new TopologyBuilder();

    Config config = new Config();
    config.setDebug(true);
    //In this example, we are registering the metrics consumer with a
    // parallelism hint of 2. Here is the line we need to add when defining the topology.
    config.registerMetricsConsumer(LoggingMetricsConsumer.class, 5);

    /*
    ----------------------TODO-----------------------
    Task: wire up the topology

    NOTE:make sure when connecting components together, using the functions setBolt(name,…) and setSpout(name,…),
    you use the following names for each component:
    FileReaderSpout -> "spout"
    SplitSentenceBolt -> "split"
    WordCountBolt -> "count"



    ------------------------------------------------- */
    builder.setSpout("spout",new FileReaderSpout(),2);
     builder.setBolt("split",new SplitSentenceBolt(),8).shuffleGrouping("spout");
    builder.setBolt("count",new WordCountBolt(),12).fieldsGrouping("split",new Fields("word"));

    config.setMaxTaskParallelism(5);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("word-count", config, builder.createTopology());

    //wait for 2 minutes and then kill the job
    Thread.sleep( 2 * 60 * 1000);

    cluster.shutdown();
  }
}
