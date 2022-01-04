import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Top3Mapper extends Mapper<Object, Text,Text, DoubleWritable> {
    private TreeMap<Double,String> map;
    private int tipo;

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
            valore = 0;
        }
        else{
            valore = Double.parseDouble(token[tipo]);
        }
        map.put(valore,nazioneEData);
        if(map.size()>3){
            map.remove(map.firstKey());
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
