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

package org.apache.spark.shuffle.celeborn.ui

import org.apache.spark.shuffle.celeborn.{CelebornWorkerSlots, ShuffleAssignmentUIData}
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(classOf[JUnit4])
class ShuffleAssignmentTablesSuite {

  private val assignments = Seq(
    ShuffleAssignmentUIData(
      shuffleId = 0,
      appShuffleId = 0,
      numPartitions = 3,
      stageEnded = true,
      Seq(CelebornWorkerSlots("w1", 2, 1, 1), CelebornWorkerSlots("w2", 1, 2, 0))),
    ShuffleAssignmentUIData(
      shuffleId = 1,
      appShuffleId = 0,
      numPartitions = 2,
      stageEnded = false,
      Seq(CelebornWorkerSlots("w2", 2, 2, 0))))

  @Test
  def shuffleTableRendersSlotsAndDetailLink(): Unit = {
    val html = ShuffleAssignmentTables.shuffleTable(assignments, id => s"/shuffle/?id=$id")
      .mkString

    assertTrue(html.contains("""<a href="/shuffle/?id=1">0</a>"""))
    assertTrue(html.contains("<td>Stage Ended</td>"))
    assertTrue(html.contains("<td>Registered</td>"))
    // Slots per worker (min / avg / max) of shuffle 0: w1 = 3, w2 = 3.
    assertTrue(html.contains("3 / 3.0 / 3"))
  }

  @Test
  def workerTableShowsShareOfSlots(): Unit = {
    val html = ShuffleAssignmentTables.workerTable(assignments.head.workers).mkString

    // w1 = 3 of 6 slots, w2 = 3 of 6 slots.
    assertTrue(html.contains("<td>w1</td>"))
    assertTrue(html.contains("50.0%"))
  }
}
