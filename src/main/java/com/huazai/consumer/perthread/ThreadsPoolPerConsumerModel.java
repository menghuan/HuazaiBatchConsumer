package com.huazai.consumer.perthread;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ThreadsPoolPerConsumerModel {
  public static void main(String[] args) throws InterruptedException {
    Properties config = new Properties();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "message3");
    // 开启自动提交
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    // 自动提交间隔时间
    config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");
    // key 反序列化
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    // value 反序列化
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    // 消费方式
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final String topic = "message3";
    int partitionNum = getTopicPartitionNum(topic);
    // 初始化线程数量
    final int threadNum = 2;
    // 计算每个消费线程持有的分区数
    int perThreadPartitionNum = (partitionNum <= threadNum) ? 1 : partitionNum / threadNum + 1;

    // 初始化线程池，根据分区数设置线程池
    ExecutorService threadPool = Executors.newFixedThreadPool(partitionNum);
    // 主题分区集合
    List<TopicPartition> topicPartitions = new ArrayList<>(perThreadPartitionNum);

    // 向线程池添加任务
    for (int i = 0; i < partitionNum; i++) {
      // 分配消费分区
      topicPartitions.add(new TopicPartition(topic, i));
      if ((i + 1) % perThreadPartitionNum == 0) {
        // 提交任务
        threadPool.submit(new WorkTask(new ArrayList<>(topicPartitions), config));
        topicPartitions.clear();
      }
    }

    if (!topicPartitions.isEmpty()) {
      // 提交任务
      threadPool.submit(new WorkTask(new ArrayList<>(topicPartitions), config));
    }

    // 关闭线程池
    threadPool.shutdown();

    // 设置线程池等待最大时长
    threadPool.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
  }

  /**
   * 工作处理线程
   */
  static class WorkTask implements Runnable {
    private final List<TopicPartition> topicPartitions;
    private final Properties config;

    public WorkTask(List<TopicPartition> topicPartitions, Properties config) {
      this.topicPartitions = topicPartitions;
      this.config = config;
    }

    @Override
    public void run() {
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config);
      System.out.println(topicPartitions);
      // 设置消费分区
      consumer.assign(topicPartitions);
      try {
        while (true) {
          // 定时拉取
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10_000));
          for (ConsumerRecord<String, String> record : records) {
            System.out.println(record);
            // 处理业务数据
            //....
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        consumer.close();
      }
    }
  }

  // 5 个分区，可自行根据生产环境修改
  public static int getTopicPartitionNum(String topic) {
    /**
     // 创建新的 TopicPartition 对象
     TopicPartition topicPartition = new TopicPartition(topic, 0);
     // 获取 TopicPartition 对应的 Topic 名称
     Map<TopicPartition, TopicDescription> topicDescriptions = adminClient.describeTopics(
     Collections.singletonList(topicPartition.topic()),
     new DescribeTopicsOptions().includePartitions()
     ).all().get();
     TopicDescription topicDescription = topicDescriptions.get(topicPartition);
     // 获取 Topic 的分区数量
     int partitionCount = topicDescription.partitions().size();
     */

    // 这里我们直接硬编码进去，如果需要动态可以参考上面的代码
    return 5;
  }
}
