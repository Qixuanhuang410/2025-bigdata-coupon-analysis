import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task4Reducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        int usedCount = 0;
        int unusedCount = 0;
        
        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String type = parts[0];
                int count = Integer.parseInt(parts[1]);
                
                if (type.equals("used")) {
                    usedCount += count;
                } else if (type.equals("unused")) {
                    unusedCount += count;
                }
            }
        }
        
        int total = usedCount + unusedCount;
        double usageRate = total > 0 ? (double) usedCount / total * 100 : 0;
        
        String output = usedCount + "\t" + unusedCount + "\t" + 
                       String.format("%.2f%%", usageRate);
        result.set(output);
        context.write(key, result);
    }
}
