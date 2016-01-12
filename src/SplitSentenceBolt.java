
import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class SplitSentenceBolt extends BaseRichBolt {
    private OutputCollector _collector;

    transient CountMetric _countMetric;
    transient MultiCountMetric _wordCountMetric;
    transient ReducedMetric _wordLengthMeanMetric;

//https://www.endgame.com/blog/storm-metrics-how
    void initMetrics(TopologyContext context)
    {
        _countMetric = new CountMetric();
        _wordCountMetric = new MultiCountMetric();
        _wordLengthMeanMetric = new ReducedMetric(new MeanReducer());
    // 3) time bucket size in seconds. The “time bucket
        // size in seconds” controls how often the metrics are sent to the Metrics Consumer.
        context.registerMetric("execute_count", _countMetric, 5);
        context.registerMetric("word_count", _wordCountMetric, 60);
        context.registerMetric("word_length", _wordLengthMeanMetric, 60);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
        //public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        //no se puede agregar el collector. No se si sea solo en los spouts que se le agrega.
        _collector=collector;
        initMetrics(context);
        }

    void updateMetrics(String word)
    {
        _countMetric.incr();
        _wordCountMetric.scope(word).incr();
        _wordLengthMeanMetric.update(word.length());
    }

    @Override
    public void execute(Tuple tuple) {
       String sentence = tuple.getString(0);
       String[]words=sentence.split("[\\s~`!@#$%^&*(-)+=_:;'\",.<>?/\\\\0-9"+"\\]\\[\\}\\{]+");

       for(String word:words){
         _collector.emit(new Values(word));

       }
        updateMetrics(tuple.getString(0));
   }
   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
     declarer.declare(new Fields("word"));
   }
 }
