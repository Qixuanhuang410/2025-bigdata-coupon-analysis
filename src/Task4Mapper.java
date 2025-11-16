import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Task4Mapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text discountType = new Text();
    private Text usageInfo = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString();
        if (line.startsWith("User_id")) return;
        
        String[] fields = line.split(",", -1);
        if (fields.length < 7) return;
        
        String discountRate = fields[3];
        String coupon = fields[2];
        String dateUsed = fields[6];
        
        if (!discountRate.equals("null") && !discountRate.isEmpty()) {
            String discountCategory = categorizeDiscount(discountRate);
            
            if (!discountCategory.equals("unknown")) {
                discountType.set(discountCategory);
                
                // 判断是否使用了优惠券
                if (!coupon.equals("null") && !coupon.isEmpty()) {
                    if (!dateUsed.equals("null") && !dateUsed.isEmpty()) {
                        usageInfo.set("used\t1"); // 使用了优惠券
                    } else {
                        usageInfo.set("unused\t1"); // 领取但未使用
                    }
                    context.write(discountType, usageInfo);
                }
            }
        }
    }
    
    private String categorizeDiscount(String discountRate) {
        if (discountRate.equals("fixed")) {
            return "fixed";
        } else if (discountRate.contains(":")) {
            return "manjian"; // 满减优惠
        } else {
            try {
                double rate = Double.parseDouble(discountRate);
                if (rate >= 0 && rate <= 1) {
                    return "discount"; // 直接折扣
                }
            } catch (NumberFormatException e) {
                // 忽略格式错误
            }
        }
        return "unknown";
    }
}
