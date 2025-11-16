import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task3Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text couponId = new Text();
    private Text intervalInfo = new Text();
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString();
        if (line.startsWith("User_id")) return;
        
        String[] fields = line.split(",", -1);
        if (fields.length < 7) return;
        
        String coupon = fields[2];
        String dateReceived = fields[5];
        String dateUsed = fields[6];
        
        // 只处理使用了优惠券的记录
        if (!coupon.equals("null") && !coupon.isEmpty() && 
            !dateReceived.equals("null") && !dateReceived.isEmpty() &&
            !dateUsed.equals("null") && !dateUsed.isEmpty()) {
            
            try {
                // 计算领取到使用的时间间隔（天）
                Date receiveDate = dateFormat.parse(dateReceived);
                Date useDate = dateFormat.parse(dateUsed);
                long interval = (useDate.getTime() - receiveDate.getTime()) / (1000 * 60 * 60 * 24);
                
                if (interval >= 0 && interval <= 15) { // 只在15天内有效
                    couponId.set(coupon);
                    intervalInfo.set(interval + "\t1"); // 间隔天数和使用次数
                    context.write(couponId, intervalInfo);
                }
            } catch (Exception e) {
                // 忽略日期解析错误
            }
        }
    }
}
