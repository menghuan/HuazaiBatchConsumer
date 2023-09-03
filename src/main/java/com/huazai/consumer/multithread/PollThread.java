package com.huazai.consumer.multithread;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PollThread implements Runnable, ConsumerRebalanceListener {
  // 消费者
  private final KafkaConsumer<String, String> consumer;
  // 工作线程池
  private final ExecutorService workerThreadPool;
  // 正在处理任务集合
  private final Map<TopicPartition, WorkerThread> doingTasks = new HashMap<>();
  // 位移提交集合
  private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
  // 原子暂停状态
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  // 上一次提交 Offset 时间
  private long lastCommitTime = System.currentTimeMillis();

  public PollThread(String topic, Properties config, ExecutorService workerThreadPool) {
    this.workerThreadPool = workerThreadPool;
    // 关闭自动提交 Offset
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumer = new KafkaConsumer<>(config);
    // 订阅 topic
    consumer.subscribe(Collections.singleton(topic), this);
  }

  @Override
  public void run() {
    try {
      // 主流程
      while (!stopped.get()) {
        // 消费数据
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10_000));
        // 按 partition 分组，分发消息数据给处理线程池
        handleRecords(records);
        // 检查正在处理的线程
        checkDoingTasks();
        // 提交offset
        commitOffsets();
      }
    } catch (WakeupException we) {
      if (!stopped.get()) {
        throw we;
      }
    } finally {
      consumer.close();
    }
  }

  //  按 partition 分组，分发消息数据给工作线程池
  private void handleRecords(ConsumerRecords<String, String> records) {
    // 判断消息数量
    if (records.count() > 0) {
      // 暂停分区集合
      List<TopicPartition> partitionsToPause = new ArrayList<>();
      // 按 Partition 分组
      records.partitions().forEach(partition -> {
        // 消费分区数据集合
        List<ConsumerRecord<String, String>> consumerRecords = records.records(partition);

        // 提交一个分区的数据给处理线程池
        WorkerThread workerThread = new WorkerThread(consumerRecords);
        workerThreadPool.submit(workerThread);

        // 记录分区与处理线程的关系，方便后面查询处理状态
        doingTasks.put(partition, workerThread);
      });
      // pause已经在处理的分区，避免同个分区的数据被多个线程同时消费，从而保证分区内数据有序处理
      consumer.pause(partitionsToPause);
    }
  }

  /**
   * 找到已经处理完成的 Partition，获取 offset 放到待提交队列里面
   */
  private void checkDoingTasks() {
    // 完成的 Partition 集合
    List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
    // 循环扫描获取
    doingTasks.forEach((partition, workerThread) -> {
      // 当工作线程完成后将该分区添加到已完成任务分区集合中
      if (workerThread.isFinished()) {
        finishedTasksPartitions.add(partition);
      }
      // 获取工作线程当前的 Offset
      long offset = workerThread.getCurrentOffset();
      if (offset > 0) {
        // 添加到待提交队列
        offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
      }
    });
    // 移除正在处理的任务
    finishedTasksPartitions.forEach(doingTasks::remove);
    // 继续消费
    consumer.resume(finishedTasksPartitions);
  }

  // 提交位移
  private void commitOffsets() {
    try {
      // 获取当前时间
      long currentMillis = System.currentTimeMillis();
      // 如果当前时间 - 上一次提交时间大于 5000 则开始提交
      if (currentMillis - lastCommitTime > 5000) {
        if (!offsetsToCommit.isEmpty()) {
          // 同步提交
          consumer.commitSync(offsetsToCommit);
          // 提交完清理集合
          offsetsToCommit.clear();
        }
        // 重置上次提交时间
        lastCommitTime = currentMillis;
      }
    } catch (Exception e) {
      log.error("Failed to commit offsets!", e);
    }
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    // 1. stop all tasks handling records from revoked partitions
    Map<TopicPartition, WorkerThread> stoppedTask = new HashMap<>();
    for (TopicPartition partition : partitions) {
      WorkerThread workerThread = doingTasks.remove(partition);
      if (workerThread != null) {
        workerThread.stop();
        stoppedTask.put(partition, workerThread);
      }
    }

    // 2. wait for stopped task to complete processing of current record
    stoppedTask.forEach((partition, workerThread) -> {
      long offset = workerThread.waitForCompletion();
      if (offset > 0) {
        offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
      }
    });

    // 3. collect offsets for revoked partitions
    Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
    for (TopicPartition partition : partitions) {
      OffsetAndMetadata offsetAndMetadata = offsetsToCommit.remove(partition);
      if (offsetAndMetadata != null) {
        revokedPartitionOffsets.put(partition, offsetAndMetadata);
      }
    }

    // 4. commit offsets for revoked partitions
    try {
      consumer.commitSync(revokedPartitionOffsets);
    } catch (Exception e) {
      log.warn("Failed to commit offsets for revoked partitions!", e);
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    // 如果分区之前没有 pause 过，那执行 resume 就不会有什么效果
    consumer.resume(partitions);
  }

  // 停止消费
  public void stopConsuming() {
    // 原子设置暂停状态
    stopped.set(true);
    // 唤醒消费者
    consumer.wakeup();
  }
}
