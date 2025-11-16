# 实验2：MapReduce - O2O优惠券使用行为分析

## 实验信息
- **课程**: 大数据技术实验
- **实验名称**: MapReduce编程实践 - O2O优惠券使用行为分析
- **姓名**: [请填写你的姓名]
- **学号**: [请填写你的学号]
- **日期**: 2025年11月

## 实验目标
1. 掌握MapReduce编程模型
2. 学习使用Hadoop处理实际业务数据
3. 分析O2O优惠券使用行为规律
4. 探索影响优惠券使用的关键因素

## 实验环境
- **操作系统**: Ubuntu 20.04 LTS
- **Hadoop版本**: 3.3.4
- **Java版本**: OpenJDK 11
- **开发工具**: Vim, Git

## 数据集说明
使用天池平台的O2O优惠券使用预测数据集：
- **数据时间范围**: 2016年1月1日 - 2016年6月30日
- **数据文件**:
  - `ccf_offline_stage1_train.csv`: 线下消费行为数据
  - `ccf_online_stage1_train.csv`: 线上消费行为数据

## 实验任务与实现

### 任务一：消费行为统计
**目标**: 统计每个商家的优惠券使用情况（正样本、负样本、普通消费）

**核心代码**:
```java
// Mapper: 分类消费行为
if (!coupon.equals("null")) {
    if (!date.equals("null")) {
        type.set("positive\t1"); // 领取并使用
    } else {
        type.set("negative\t1"); // 领取未使用
    }
} else {
    if (!date.equals("null")) {
        type.set("normal\t1"); // 普通消费
    }
}
