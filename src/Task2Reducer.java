import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task2Reducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        Map<String, Integer> distanceCount = new HashMap<>();
        
        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String distance = parts[0];
                int count = Integer.parseInt(parts[1]);
                
                distanceCount.put(distance, distanceCount.getOrDefault(distance, 0) + count);
            }
        }
        
        for (Map.Entry<String, Integer> entry : distanceCount.entrySet()) {
            String output = entry.getKey() + "\t" + entry.getValue();
            result.set(output);
            context.write(key, result);
        }
    }
}
