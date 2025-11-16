import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task1OfflineMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text merchantId = new Text();
    private Text type = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString();
        if (line.startsWith("User_id")) return;
        
        String[] fields = line.split(",", -1);
        if (fields.length < 7) return;
        
        String merchant = fields[1];
        String coupon = fields[2];
        String date = fields[6];
        
        if (!merchant.equals("null") && !merchant.isEmpty()) {
            merchantId.set(merchant);
            
            if (!coupon.equals("null") && !coupon.isEmpty()) {
                if (!date.equals("null") && !date.isEmpty()) {
                    type.set("positive\t1");
                } else {
                    type.set("negative\t1");
                }
            } else {
                if (!date.equals("null") && !date.isEmpty()) {
                    type.set("normal\t1");
                }
            }
            
            context.write(merchantId, type);
        }
    }
}
