/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.celeborn

import java.io.{File, FileInputStream}

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.Status.ASYNC_TRACKING_ENABLED
import org.apache.spark.scheduler.ReplayListenerBus
import org.apache.spark.sql.SparkSession
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.InMemoryStore
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.{interval, timeout}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.tests.spark.SparkTestBase

class CelebornUIShuffleAssignmentSuite extends SparkTestBase {

  override def beforeAll(): Unit = {
    // Flush every push so that the tiny split threshold below is hit and slots are reallocated.
    setupMiniClusterWithRandomPorts(
      workerConf = Map(CelebornConf.WORKER_FLUSHER_BUFFER_SIZE.key -> "0"),
      workerNum = 3)
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  test("slot assignments are recorded live and restored from the event log") {
    val eventLogDir = Utils.createTempDir()
    val sparkConf = updateSparkConf(
      new SparkConf().setAppName("celeborn-ui-shuffle-assignment").setMaster("local[2]"),
      ShuffleMode.HASH)
      .set("spark.plugins", classOf[CelebornPlugin].getName)
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", eventLogDir.toURI.toString)
      .set("spark.eventLog.compress", "false")
      .set("spark.eventLog.rolling.enabled", "false")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "true")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key}", "4k")
      .set(s"spark.${CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key}", "1k")
      // A hard split always revives to a new location, a soft split may be skipped at mapper end.
      .set(s"spark.${CelebornConf.SHUFFLE_PARTITION_SPLIT_MODE.key}", "HARD")
    val numPartitions = 6
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val liveAssignments =
      try {
        val sc = spark.sparkContext
        val counts = sc.parallelize(1 to 20000, 4)
          .map(i => (i % 100, i.toString))
          .groupByKey(numPartitions)
          .mapValues(_.size)
          .collect()
        assert(counts.map(_._2).sum == 20000)

        val statusStore = new CelebornStatusStore(sc.statusStore.store)
        eventually(timeout(30.seconds), interval(100.milliseconds)) {
          val assignments = statusStore.shuffleAssignments()
          assert(assignments.size == 1)
          assert(assignments.head.stageEnded)
        }
        statusStore.shuffleAssignments()
      } finally {
        spark.stop()
      }

    val assignment = liveAssignments.head
    assert(assignment.appShuffleId == 0)
    assert(assignment.numPartitions == numPartitions)
    assert(assignment.workers.nonEmpty && assignment.workers.size <= 3)
    val primarySlots = assignment.workers.map(_.primarySlots).sum
    val replicaSlots = assignment.workers.map(_.replicaSlots).sum
    val reallocatedSlots = assignment.workers.map(_.reallocatedSlots).sum
    assert(reallocatedSlots > 0, "the split threshold should have reallocated slots")
    assert(primarySlots == numPartitions + reallocatedSlots)
    assert(replicaSlots == primarySlots)

    // History Server replay of the recorded events restores the same assignment.
    val replayConf = new SparkConf().set(ASYNC_TRACKING_ENABLED, false)
    val replayStore = new ElementTrackingStore(new InMemoryStore(), replayConf)
    val replayBus = new ReplayListenerBus()
    new CelebornHistoryServerPlugin().createListeners(replayConf, replayStore)
      .foreach(replayBus.addListener)
    val eventLogs = eventLogDir.listFiles().filter(_.isFile)
    assert(eventLogs.length == 1)
    val eventLog: File = eventLogs.head
    Utils.tryWithResource(new FileInputStream(eventLog)) { in =>
      replayBus.replay(in, eventLog.getName)
    }
    val replayed = new CelebornStatusStore(replayStore)
    assert(replayed.extensionEnabled())
    assert(replayed.shuffleAssignments() == liveAssignments)
  }
}
