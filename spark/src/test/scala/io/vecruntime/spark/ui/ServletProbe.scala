/*
 * Copyright 2025-2026 Angel Conde and the vecruntime contributors
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
package io.vecruntime.spark.ui

import java.io.ByteArrayOutputStream
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.charset.StandardCharsets

import jakarta.servlet.{ServletOutputStream, WriteListener}
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.spark.sql.vecruntime.ui.VectorAccelerationTab

/** Calls a servlet's GET with stub request/response objects, without a server. */
object ServletProbe {
  final case class Result(status: Int, contentType: String, body: String)

  def get(servlet: VectorAccelerationTab.StaticServlet, pathInfo: String): Result = {
    val req = proxy[HttpServletRequest] { (m, _) =>
      m.getName match {
        case "getPathInfo" => pathInfo
        case _ => null
      }
    }
    val body = new ByteArrayOutputStream()
    val out = new ServletOutputStream {
      override def isReady: Boolean = true
      override def setWriteListener(l: WriteListener): Unit = ()
      override def write(b: Int): Unit = body.write(b)
    }
    var status = 200
    var contentType = ""
    val resp = proxy[HttpServletResponse] { (m, args) =>
      m.getName match {
        case "getOutputStream" => out
        case "setContentType" => contentType = args(0).asInstanceOf[String]; null
        case "sendError" => status = args(0).asInstanceOf[Int]; null
        case "setStatus" => status = args(0).asInstanceOf[Int]; null
        case _ => null
      }
    }
    servlet.doGet(req, resp)
    Result(status, contentType, new String(body.toByteArray, StandardCharsets.UTF_8))
  }

  private def proxy[T](f: (Method, Array[AnyRef]) => AnyRef)(implicit ct: scala.reflect.ClassTag[T]): T =
    Proxy
      .newProxyInstance(
        getClass.getClassLoader,
        Array[Class[_]](ct.runtimeClass),
        new InvocationHandler {
          override def invoke(p: AnyRef, m: Method, args: Array[AnyRef]): AnyRef = f(m, args)
        }
      )
      .asInstanceOf[T]
}
