/*
 * Copyright 2026-2027 Angel Conde and the vecruntime contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.vecruntime.ui

import scala.xml.Node

import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, SparkUITab}

/**
 * The "Vector Acceleration" tab: a list of SQL executions and, per execution, the physical plan
 * as a DAG with each operator coloured by the engine that runs it.
 *
 * Lives under `org.apache.spark` because `SparkUITab`, `WebUIPage` and `UIUtils` are all
 * `private[spark]`; Spark exposes no public API for adding a tab.
 */
class VectorAccelerationTab(val store: VectorAccelerationStore, sparkUI: SparkUI)
    extends SparkUITab(sparkUI, "vector") with Logging {

  override val name: String = "Vector Acceleration"

  def conf: SparkConf = sparkUI.conf
  val parent: SparkUI = sparkUI

  attachPage(new AllExecutionsPage(this))
  attachPage(new ExecutionPlanPage(this))
  parent.attachTab(this)
  // Not `addStaticHandler`: Spark's static handler resolves the directory with Spark's own class
  // loader, which does not see a plugin on `--packages` / `--jars`. This servlet reads through ours.
  UiHandlers.attachServlet(
    parent,
    VectorAccelerationTab.StaticPath,
    new VectorAccelerationTab.StaticServlet(
      VectorAccelerationTab.StaticResourceDir,
      VectorAccelerationTab.getClass.getClassLoader
    ),
    "/*"
  )

  /** After the SQL tab (0), before the rest. */
  override def displayOrder: Int = 1
}

object VectorAccelerationTab {
  val StaticResourceDir = "org/apache/spark/sql/vecruntime/ui/static"
  val StaticPath = "/static/vector"

  /** The files the tab serves, and their content types. Anything else is a 404. */
  val StaticFiles: Map[String, String] = Map(
    "vector-acceleration.css" -> "text/css;charset=utf-8",
    "vector-acceleration.js" -> "text/javascript;charset=utf-8"
  )

  /**
   * Serves [[StaticFiles]] from `resourceDir` through `loader` -- the plugin's class loader. Spark's
   * `WebUI.addStaticHandler` resolves the directory through `Utils.getSparkClassLoader`, which only
   * sees `$SPARK_HOME/jars`, so with the plugin on `--packages` or `--jars` (a child loader) the CSS
   * and JS were 404 and the tab rendered unstyled, without its plan DAG. Only the listed names are
   * served (no path is ever built from the request beyond a known file name).
   */
  class StaticServlet(resourceDir: String, loader: ClassLoader) extends HttpServlet {
    override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
      val name = Option(req.getPathInfo).getOrElse("").stripPrefix("/")
      val found =
        StaticFiles.get(name).flatMap(ct => Option(loader.getResourceAsStream(s"$resourceDir/$name")).map(ct -> _))
      found match {
        case Some((contentType, in)) =>
          try {
            val bytes = in.readAllBytes()
            resp.setContentType(contentType)
            resp.setHeader("Cache-Control", "max-age=3600")
            resp.setContentLength(bytes.length)
            resp.getOutputStream.write(bytes)
          } finally in.close()
        case None => resp.sendError(HttpServletResponse.SC_NOT_FOUND)
      }
    }
  }

  /** Shared legend, so the list and the plan page agree on what each colour means. */
  def legend: Seq[Node] =
    <div class="sv-legend">
      {
      Engine.all.map { e =>
        <span class="sv-legend-item">
          <span class={s"sv-legend-swatch ${e.cssClass}"}></span>
          <span class="sv-legend-label">{e.label}</span>
        </span>
      }
    }
    </div>

  /**
   * The badge shown when nothing in the plan fell back to Spark. Otherwise the accelerated share,
   * with a tooltip separating operators we tried and failed to convert (a recorded reason) from
   * operators that were never candidates, such as a scan, an exchange or a sort.
   */
  def fullyAcceleratedBadge(plan: AcceleratedPlan): Seq[Node] =
    if (plan.fullyAccelerated) {
      <span class="sv-badge sv-badge-full" title="Every operator in this plan runs on the Vector API or on Comet">
        Fully Accelerated
      </span>
    } else {
      val onSpark = plan.countBy(Engine.Spark)
      val missed = plan.fallbacks.size
      val tip =
        s"$onSpark operator(s) run on Spark; $missed of them were candidates we could not convert " +
          s"(see the reasons on the plan page)"
      <span class="sv-badge sv-badge-partial" title={tip}>
        {f"${plan.acceleratedFraction * 100}%.0f%% accelerated"}
      </span>
    }

  def resourceUri(request: HttpServletRequest, file: String): String =
    org.apache.spark.ui.UIUtils.prependBaseUri(request, s"/static/vector/$file")
}
