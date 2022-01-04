import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.TreeMap;

public class prova {
    public static void main(String [] args) throws FileNotFoundException, IOException {

        TreeMap<Double,String> map = new TreeMap<>();
        BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\giugi\\Desktop\\Java\\ciao.txt"));
        String linea = reader.readLine();


        while(linea!=null) {
            String[] token = linea.split(",");
            double valore;
            String nazioneEData=token[0]+" "+token[2];
            if(token[7].equals("")|| token[7].equals(null)){
                valore = 0;
            }
            else{
                valore = Double.parseDouble(token[7]);
            }
            map.put(valore,nazioneEData);
            if(map.firstKey() == 0 || map.firstKey() == 0.0 || map.firstKey() == 0.00){
                map.remove(map.firstKey());
            }
            if(map.size()>3){
                map.remove(map.lastKey());
            }

            linea = reader.readLine();
        }
        System.out.println(map.toString());
    }

}
