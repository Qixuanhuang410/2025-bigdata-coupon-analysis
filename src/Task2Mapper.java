import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString();
        if (line.startsWith("User_id")) return;
        
        String[] fields = line.split(",", -1);
        if (fields.length < 7) return;
        
        String merchant = fields[1];
        String distance = fields[4];
        
        if (!merchant.equals("null") && !merchant.isEmpty() && 
            !distance.equals("null") && !distance.isEmpty()) {
            
            outputKey.set(merchant);
            outputValue.set(distance + "\t1");
            context.write(outputKey, outputValue);
        }
    }
}
