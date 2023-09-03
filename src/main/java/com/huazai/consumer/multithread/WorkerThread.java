package com.huazai.consumer.multithread;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
// 工作线程
public class WorkerThread implements Runnable {
  private final List<ConsumerRecord<String, String>> records;
  // 当前位移
  private final AtomicLong currentOffset = new AtomicLong();
  // 是否暂停
  private volatile boolean stopped = false;
  // 是否继续
  private volatile boolean started = false;
  // 是否完成
  private volatile boolean finished = false;
  private final CompletableFuture<Long> completion = new CompletableFuture<>();
  private final ReentrantLock startStopLock = new ReentrantLock();

  public WorkerThread(List<ConsumerRecord<String, String>> records) {
    this.records = records;
  }

  @Override
  public void run() {
    // 加锁
    startStopLock.lock();
    try {
      // 如果暂停直接返回
      if (stopped) {
        return;
      }
      // 开启
      started = true;
    } finally {
      // 释放锁
      startStopLock.unlock();
    }

    // 循环分配到的消费数据
    for (ConsumerRecord<String, String> record : records) {
      if (stopped) {
        break;
      }
      // process record here and make sure you catch all exceptions;
      System.out.println(Thread.currentThread().getName() + ":" + record);
      // 这里添加自己的业务逻辑
      //....
      // 设置位移值
      currentOffset.set(record.offset() + 1);
    }
    // 当循环完成后，设置任务完成
    finished = true;
    //
    completion.complete(currentOffset.get());
  }

  // 获取当前位移
  public long getCurrentOffset() {
    return currentOffset.get();
  }

  // 暂停消费
  public void stop() {
    startStopLock.lock();
    try {
      this.stopped = true;
      if (!started) {
        finished = true;
        completion.complete(currentOffset.get());
      }
    } finally {
      startStopLock.unlock();
    }
  }

  // 等待完成当前正在处理的消息
  public long waitForCompletion() {
    try {
      return completion.get();
    } catch (InterruptedException | ExecutionException e) {
      return -1;
    }
  }

  // 是否完成
  public boolean isFinished() {
    return finished;
  }
}
