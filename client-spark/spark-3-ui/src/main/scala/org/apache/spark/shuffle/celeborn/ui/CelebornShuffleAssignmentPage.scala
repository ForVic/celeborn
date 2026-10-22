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

import scala.util.Try
import scala.xml.Node

import org.apache.spark.shuffle.celeborn.ui.SparkServletBridge.HttpServletRequest
import org.apache.spark.ui.{UIUtils, WebUIPage}

import org.apache.celeborn.common.CelebornConf

/** Per-worker slots of one shuffle, addressed by its Celeborn shuffle id. */
private[celeborn] class CelebornShuffleAssignmentPage(parent: CelebornUITab)
  extends WebUIPage("shuffle") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")
    val shuffleId = Try(parameterId.toInt).getOrElse(
      throw new IllegalArgumentException(s"Invalid shuffle id: $parameterId"))

    val content = parent.store.shuffleAssignment(shuffleId) match {
      case Some(assignment) =>
        <div>
          <ul class="list-unstyled">
            <li><strong>Shuffle ID: </strong>{assignment.appShuffleId}</li>
            <li><strong>Celeborn Shuffle ID: </strong>{assignment.shuffleId}</li>
            <li><strong>Status: </strong>{ShuffleAssignmentTables.status(assignment)}</li>
            <li><strong>Partitions: </strong>{assignment.numPartitions}</li>
            <li>
              <strong>Slots: </strong>
              {
          s"${assignment.workers.map(_.primarySlots).sum} primary, " +
            s"${assignment.workers.map(_.replicaSlots).sum} replica on " +
            ShuffleAssignmentTables.plural(assignment.workers.size, "worker")
        }
            </li>
            <li>
              <strong>Reallocated (split/revive): </strong>
              {assignment.workers.map(_.reallocatedSlots).sum}
            </li>
          </ul>
          <h4>Worker Slots</h4>
          {
          ShuffleAssignmentTables.workerTable(assignment.workers)
        }
        </div>
      case None =>
        <div class="alert alert-info">
          No slot assignment is recorded for Celeborn shuffle {shuffleId}. The number of
          retained shuffles is limited by spark.{CelebornConf.CLIENT_SPARK_UI_RETAINED_SHUFFLES.key}.
        </div>
    }
    UIUtils.headerSparkPage(request, s"Celeborn Shuffle $shuffleId", content, parent)
  }
}
