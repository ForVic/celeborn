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

import scala.xml.Node

import org.apache.spark.shuffle.celeborn.{CelebornWorkerSlots, ShuffleAssignmentUIData}
import org.apache.spark.ui.UIUtils

/** Renders the slot assignment tables of the Celeborn tab. */
private[ui] object ShuffleAssignmentTables {

  private val reallocatedTooltip = Some("Primary slots allocated by partition splits and revives.")

  def plural(count: Int, noun: String): String = s"$count $noun${if (count == 1) "" else "s"}"

  def status(assignment: ShuffleAssignmentUIData): String =
    if (assignment.stageEnded) "Stage Ended" else "Registered"

  def shuffleTable(
      assignments: Seq[ShuffleAssignmentUIData],
      shuffleUrl: Int => String): Seq[Node] = {
    UIUtils.listingTable(
      Seq(
        "Shuffle ID",
        "Celeborn Shuffle ID",
        "Status",
        "Partitions",
        "Workers",
        "Primary Slots",
        "Replica Slots",
        "Reallocated Slots",
        "Slots per Worker (Min / Avg / Max)"),
      shuffleRow(shuffleUrl),
      assignments,
      id = Some("celeborn-shuffle-assignments"),
      headerClasses = Seq(
        "sorttable_numeric",
        "sorttable_numeric",
        "sorttable_alpha",
        "sorttable_numeric",
        "sorttable_numeric",
        "sorttable_numeric",
        "sorttable_numeric",
        "sorttable_numeric",
        "sorttable_numeric"),
      tooltipHeaders = Seq(
        Some("Spark shuffle id. Links to the per-worker slots of the shuffle."),
        Some("May differ from the Spark shuffle id when stage rerun is enabled."),
        Some("Registered: slots reserved at shuffle registration. Stage Ended: final " +
          "assignment, including slots reallocated by splits and revives."),
        None,
        None,
        None,
        None,
        reallocatedTooltip,
        Some("Primary plus replica slots per worker. A maximum far above the average " +
          "indicates an uneven assignment.")))
  }

  /** Per-worker slots of one shuffle, most slots first. */
  def workerTable(workers: Seq[CelebornWorkerSlots]): Seq[Node] = {
    def slots(w: CelebornWorkerSlots): Int = w.primarySlots + w.replicaSlots
    val total = workers.map(slots(_).toLong).sum
    UIUtils.listingTable(
      Seq("Worker", "Primary Slots", "Replica Slots", "Reallocated Slots", "Share of Slots"),
      (w: CelebornWorkerSlots) =>
        <tr>
          <td>{w.workerId}</td>
          <td>{w.primarySlots}</td>
          <td>{w.replicaSlots}</td>
          <td>{w.reallocatedSlots}</td>
          <td sorttable_customkey={slots(w).toString}>{share(slots(w), total)}</td>
        </tr>,
      workers.sortBy(w => (-slots(w), w.workerId)),
      id = Some("celeborn-shuffle-worker-slots"),
      headerClasses = "sorttable_alpha" +: Seq.fill(4)("sorttable_numeric"),
      tooltipHeaders = Seq(
        Some("host:rpcPort:pushPort:fetchPort:replicatePort"),
        None,
        None,
        reallocatedTooltip,
        Some("Primary plus replica slots over all slots of the shuffle.")))
  }

  private def shuffleRow(shuffleUrl: Int => String)(assignment: ShuffleAssignmentUIData)
      : Seq[Node] = {
    val perWorker = assignment.workers.map(w => w.primarySlots + w.replicaSlots)
    val maxPerWorker = if (perWorker.isEmpty) 0 else perWorker.max
    val perWorkerSummary =
      if (perWorker.isEmpty) {
        "N/A"
      } else {
        f"${perWorker.min} / ${perWorker.sum.toDouble / perWorker.size}%.1f / $maxPerWorker"
      }
    <tr>
      <td sorttable_customkey={assignment.appShuffleId.toString}>
        <a href={shuffleUrl(assignment.shuffleId)}>{assignment.appShuffleId}</a>
      </td>
      <td>{assignment.shuffleId}</td>
      <td>{status(assignment)}</td>
      <td>{assignment.numPartitions}</td>
      <td>{assignment.workers.size}</td>
      <td>{assignment.workers.map(_.primarySlots.toLong).sum}</td>
      <td>{assignment.workers.map(_.replicaSlots.toLong).sum}</td>
      <td>{assignment.workers.map(_.reallocatedSlots.toLong).sum}</td>
      <td sorttable_customkey={maxPerWorker.toString}>{perWorkerSummary}</td>
    </tr>
  }

  private def share(part: Long, total: Long): String =
    if (total <= 0) "N/A" else f"${part.toDouble * 100.0 / total}%.1f%%"
}
