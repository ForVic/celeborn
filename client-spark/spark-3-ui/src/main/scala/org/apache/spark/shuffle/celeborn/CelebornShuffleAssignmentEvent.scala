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

import org.apache.spark.scheduler.SparkListenerEvent

import org.apache.celeborn.client.ShuffleAssignment

/** Slots reserved on one worker, see [[org.apache.celeborn.client.WorkerSlots]]. */
private[celeborn] case class CelebornWorkerSlots(
    workerId: String,
    primarySlots: Int,
    replicaSlots: Int,
    reallocatedSlots: Int)

/**
 * Partition-to-worker slot assignment of a shuffle, posted when the shuffle is registered and
 * again with the final assignment at stage end. Recorded in the event log; the listener writes it
 * to the KVStore both for the live UI and on History Server replay.
 */
private[celeborn] case class CelebornShuffleAssignmentEvent(
    shuffleId: Int,
    appShuffleId: Int,
    numPartitions: Int,
    stageEnded: Boolean,
    workers: Seq[CelebornWorkerSlots])
  extends SparkListenerEvent

private[celeborn] object CelebornShuffleAssignmentEvent {

  def from(assignment: ShuffleAssignment): CelebornShuffleAssignmentEvent = {
    CelebornShuffleAssignmentEvent(
      assignment.shuffleId,
      assignment.appShuffleId,
      assignment.numPartitions,
      assignment.stageEnded,
      assignment.workers.map { w =>
        CelebornWorkerSlots(w.workerId, w.primarySlots, w.replicaSlots, w.reallocatedSlots)
      })
  }
}
