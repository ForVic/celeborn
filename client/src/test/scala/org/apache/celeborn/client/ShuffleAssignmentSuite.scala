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

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.meta.{ShufflePartitionLocationInfo, WorkerInfo}
import org.apache.celeborn.common.protocol.PartitionLocation

class ShuffleAssignmentSuite extends CelebornFunSuite {

  private val worker1 = new WorkerInfo("host1", 1, 2, 3, 4)
  private val worker2 = new WorkerInfo("host2", 1, 2, 3, 4)
  private val worker3 = new WorkerInfo("host3", 1, 2, 3, 4)

  private def location(
      partitionId: Int,
      epoch: Int,
      mode: PartitionLocation.Mode): PartitionLocation = {
    new PartitionLocation(partitionId, epoch, "host", 1, 2, 3, 4, mode)
  }

  private def locationInfo(
      worker: WorkerInfo,
      primary: Seq[(Int, Int)],
      replica: Seq[(Int, Int)]): ShufflePartitionLocationInfo = {
    val info = new ShufflePartitionLocationInfo(worker)
    info.addPrimaryPartitions(primary.map { case (id, epoch) =>
      location(id, epoch, PartitionLocation.Mode.PRIMARY)
    }.asJava)
    info.addReplicaPartitions(replica.map { case (id, epoch) =>
      location(id, epoch, PartitionLocation.Mode.REPLICA)
    }.asJava)
    info
  }

  test("snapshot counts slots per worker, including split and revive epochs") {
    val allocatedWorkers = new util.HashMap[String, ShufflePartitionLocationInfo]()
    // Partition 0 split once on worker2; partition 2 was revived onto worker1.
    allocatedWorkers.put(
      worker2.toUniqueId,
      locationInfo(worker2, Seq(0 -> 0, 0 -> 1, 1 -> 0), Seq(2 -> 0)))
    allocatedWorkers.put(
      worker1.toUniqueId,
      locationInfo(worker1, Seq(2 -> 0, 2 -> 1), Seq(0 -> 0, 0 -> 1, 1 -> 0, 2 -> 1)))
    // Workers left without slots, e.g. after their locations were cleared, are omitted.
    allocatedWorkers.put(worker3.toUniqueId, new ShufflePartitionLocationInfo(worker3))

    val assignment = ShuffleAssignment.snapshot(5, 2, stageEnded = true, allocatedWorkers)

    assert(assignment == ShuffleAssignment(
      shuffleId = 5,
      appShuffleId = 2,
      numPartitions = 3,
      stageEnded = true,
      workers = Seq(
        WorkerSlots(worker1.toUniqueId, primarySlots = 2, replicaSlots = 4, reallocatedSlots = 1),
        WorkerSlots(worker2.toUniqueId, primarySlots = 3, replicaSlots = 1, reallocatedSlots = 1))))
  }
}
