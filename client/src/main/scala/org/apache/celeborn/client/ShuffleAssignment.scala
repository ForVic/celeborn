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

package org.apache.celeborn.client

import java.util

import scala.collection.JavaConverters._

import org.apache.celeborn.common.meta.ShufflePartitionLocationInfo

/**
 * Slots reserved on one worker for a shuffle.
 *
 * @param workerId         [[org.apache.celeborn.common.meta.WorkerInfo.toUniqueId]] of the worker
 * @param primarySlots     primary partition locations, including split and revive epochs
 * @param replicaSlots     replica partition locations, including split and revive epochs
 * @param reallocatedSlots primary locations allocated by a split or revive
 */
case class WorkerSlots(
    workerId: String,
    primarySlots: Int,
    replicaSlots: Int,
    reallocatedSlots: Int)

/**
 * Partition-to-worker slot assignment of a shuffle. Reported once the shuffle is registered, and
 * again at stage end with the final assignment including split and revive reallocations.
 */
case class ShuffleAssignment(
    shuffleId: Int,
    appShuffleId: Int,
    numPartitions: Int,
    stageEnded: Boolean,
    workers: Seq[WorkerSlots])

object ShuffleAssignment {

  def snapshot(
      shuffleId: Int,
      appShuffleId: Int,
      stageEnded: Boolean,
      allocatedWorkers: util.Map[String, ShufflePartitionLocationInfo]): ShuffleAssignment = {
    val partitionIds = new util.HashSet[Integer]()
    val workers = allocatedWorkers.asScala.toSeq.flatMap {
      case (workerId, locationInfo) =>
        val primary = locationInfo.getPrimaryPartitions().asScala
        val replica = locationInfo.getReplicaPartitions().asScala
        primary.foreach(location => partitionIds.add(location.getId))
        if (primary.isEmpty && replica.isEmpty) {
          None
        } else {
          Some(WorkerSlots(workerId, primary.size, replica.size, primary.count(_.getEpoch > 0)))
        }
    }.sortBy(_.workerId)
    ShuffleAssignment(shuffleId, appShuffleId, partitionIds.size, stageEnded, workers)
  }
}
