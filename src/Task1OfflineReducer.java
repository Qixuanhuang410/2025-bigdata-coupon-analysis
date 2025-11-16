import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1OfflineReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        int negativeCount = 0;
        int normalCount = 0;
        int positiveCount = 0;
        
        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                String type = parts[0];
                switch (type) {
                    case "negative": negativeCount++; break;
                    case "normal": normalCount++; break;
                    case "positive": positiveCount++; break;
                }
            }
        }
        
        String output = negativeCount + "\t" + normalCount + "\t" + positiveCount;
        result.set(output);
        context.write(key, result);
    }
}
