import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Flop3Mapper extends Mapper<Object, Text,Text, DoubleWritable> {
    private TreeMap<Double,String> map;
    private int tipo;
    public final double MaxValue = 1.7976931348623157E+308;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        map = new TreeMap<Double,String>();
        Configuration c = context.getConfiguration();
        tipo = getTipo(c.get("tipologia.analisi"));
    }
    private int getTipo(String s){
        int x = Integer.parseInt(s);
        if(x==1){
            return 7; //vaccinati giornalieri
        }else {
            return 10;//rapporto.
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String linea = value.toString();
        String[] token = linea.split(",");
        double valore;
        String nazioneEData=token[0]+" "+token[2];
        if(token[tipo].equals("")|| token[tipo].equals(null)){
            valore = MaxValue;
        }
        else{
            valore = Double.parseDouble(token[tipo]);
        }
        map.put(valore,nazioneEData);
        if(map.firstKey() == 0 || map.firstKey() == 0.0 || map.firstKey() == 0.00){
            map.remove(map.firstKey());
        }
        if(map.size()>3){
            map.remove(map.lastKey());
        }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Double,String>entry : map.entrySet()){
            double valore = entry.getKey();
            String nazioneEData = entry.getValue();
            context.write(new Text(nazioneEData),new DoubleWritable(valore));
        }
    }
}
