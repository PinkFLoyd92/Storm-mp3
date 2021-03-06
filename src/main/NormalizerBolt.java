package main;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A bolt that normalizes the words, by removing common words and making them lower case.
 */
public class NormalizerBolt extends BaseBasicBolt {
    private List<String> commonWords = Arrays.asList("the", "be", "a", "an", "and",
                                                     "of", "to", "in", "am", "is", "are", "at", "not", "that", "have", "i", "it",
                                                     "for", "on", "with", "he", "she", "as", "you", "do", "this", "but", "his",
                                                     "by", "from", "they", "we", "her", "or", "will", "my", "one", "all", "s", "if",
                                                     "any", "our", "may", "your", "these", "d" , " ", "me" , "so" , "what" , "him" );

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        /*
          ----------------------TODO-----------------------
          Task:
          1. make the words all lower case
          2. remove the common words

          ------------------------------------------------- */
        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");
        String[] cmd = {
                "/bin/bash",
                "-c",
                "python3 /home/sebas/mp3_python/main.py '"
        };
        try {
            Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (String word : words) {
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                if(!this.checkWord(word))
                    collector.emit(new Values(word));
                try {
                    Runtime.getRuntime().exec(cmd);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
    public boolean checkWord(String word_to_check)
    {
	for (String word: commonWords ) {
	    if (word_to_check.equals(word))
		return true;
	}
	return false;

    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word"));

    }
}
