import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Task3Reducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    private Map<String, CouponStats> couponStatsMap = new HashMap<>();
    
    static class CouponStats {
        int totalUses = 0;
        long totalInterval = 0;
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        CouponStats stats = new CouponStats();
        
        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                long interval = Long.parseLong(parts[0]);
                int count = Integer.parseInt(parts[1]);
                
                stats.totalUses += count;
                stats.totalInterval += interval * count;
            }
        }
        
        if (stats.totalUses > 0) {
            couponStatsMap.put(key.toString(), stats);
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 计算总使用次数
        int totalUsage = 0;
        for (CouponStats stats : couponStatsMap.values()) {
            totalUsage += stats.totalUses;
        }
        
        // 计算1%阈值
        int threshold = (int) (totalUsage * 0.01);
        
        // 存储符合条件的结果
        Map<Double, String> sortedResults = new TreeMap<>();
        
        for (Map.Entry<String, CouponStats> entry : couponStatsMap.entrySet()) {
            String couponId = entry.getKey();
            CouponStats stats = entry.getValue();
            
            // 只统计使用次数大于总使用次数1%的优惠券
            if (stats.totalUses > threshold) {
                double avgInterval = (double) stats.totalInterval / stats.totalUses;
                sortedResults.put(avgInterval, couponId);
            }
        }
        
        // 按平均间隔排序输出
        for (Map.Entry<Double, String> entry : sortedResults.entrySet()) {
            String couponId = entry.getValue();
            double avgInterval = entry.getKey();
            
            result.set(String.format("%.2f", avgInterval));
            context.write(new Text(couponId), result);
        }
    }
}
