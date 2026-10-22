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

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate
import org.apache.spark.status.KVUtils.KVStoreScalaSerializer
import org.apache.spark.util.kvstore.InMemoryStore
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import org.apache.celeborn.common.CelebornConf

@RunWith(classOf[JUnit4])
class CelebornShuffleAssignmentSuite {

  private def assignmentEvent(
      shuffleId: Int,
      stageEnded: Boolean,
      reallocatedSlots: Int = 0): CelebornShuffleAssignmentEvent = {
    CelebornShuffleAssignmentEvent(
      shuffleId,
      appShuffleId = shuffleId + 100,
      numPartitions = 4,
      stageEnded,
      Seq(
        CelebornWorkerSlots("host1:1:2:3:4", 2, 2, reallocatedSlots),
        CelebornWorkerSlots("host2:1:2:3:4", 2, 2, 0)))
  }

  @Test
  def stageEndReplacesRegistrationAssignment(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf())

    listener.onOtherEvent(assignmentEvent(3, stageEnded = false))
    val registered = statusStore.shuffleAssignment(3).get
    assertEquals(103, registered.appShuffleId)
    assertEquals(false, registered.stageEnded)

    listener.onOtherEvent(assignmentEvent(3, stageEnded = true, reallocatedSlots = 1))
    val ended = statusStore.shuffleAssignment(3).get
    assertEquals(true, ended.stageEnded)
    assertEquals(1, ended.workers.head.reallocatedSlots)
    assertEquals(Seq(3), statusStore.shuffleAssignments().map(_.shuffleId))
  }

  @Test
  def assignmentIgnoredWithoutPluginOptIn(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val listener = new CelebornListener(store, new SparkConf(), requirePluginOptIn = true)

    listener.onEnvironmentUpdate(SparkListenerEnvironmentUpdate(Map("Spark Properties" -> Nil)))
    listener.onOtherEvent(assignmentEvent(0, stageEnded = false))

    assertTrue(statusStore.shuffleAssignments().isEmpty)
  }

  @Test
  def oldestAssignmentsDroppedBeyondRetainedShuffles(): Unit = {
    val store = new InMemoryStore()
    val statusStore = new CelebornStatusStore(store)
    val conf = new SparkConf()
      .set(s"spark.${CelebornConf.CLIENT_SPARK_UI_RETAINED_SHUFFLES.key}", "2")
    val listener = new CelebornListener(store, conf)

    (0 until 3).foreach(id => listener.onOtherEvent(assignmentEvent(id, stageEnded = false)))
    assertEquals(Seq(1, 2), statusStore.shuffleAssignments().map(_.shuffleId))

    // A late stage end of a dropped shuffle does not evict a newer one.
    listener.onOtherEvent(assignmentEvent(0, stageEnded = true))
    assertEquals(Seq(1, 2), statusStore.shuffleAssignments().map(_.shuffleId))
  }

  @Test
  def uiDataKVStoreSerializerRoundTrip(): Unit = {
    // History Server disk stores persist UI data through this serializer.
    val serializer = new KVStoreScalaSerializer()
    val event = assignmentEvent(5, stageEnded = true, reallocatedSlots = 2)
    val uiData = ShuffleAssignmentUIData(
      event.shuffleId,
      event.appShuffleId,
      event.numPartitions,
      event.stageEnded,
      event.workers)

    assertEquals(
      uiData,
      serializer.deserialize(serializer.serialize(uiData), classOf[ShuffleAssignmentUIData]))
  }
}
