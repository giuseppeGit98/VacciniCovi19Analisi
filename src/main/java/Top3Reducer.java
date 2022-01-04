import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Top3Reducer extends Reducer<Text,DoubleWritable, DoubleWritable,Text> {
    private TreeMap<Double,String> map2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        map2= new TreeMap<Double, String>();
    }


    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        String nazioneEData=key.toString();
        double val = 0;
        for(DoubleWritable v : values){
            val = v.get();
        }
        map2.put(val,nazioneEData);
        if(map2.size() > 3){
            map2.remove(map2.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Double,String> en : map2.entrySet()){
            double valore = en.getKey();
            String nazioneEData = en.getValue();
            context.write(new DoubleWritable(valore),new Text(nazioneEData));
        }
    }
}
