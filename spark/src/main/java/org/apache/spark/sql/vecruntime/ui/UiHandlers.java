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
package org.apache.spark.sql.vecruntime.ui;

import jakarta.servlet.http.HttpServlet;
import org.apache.spark.ui.SparkUI;

/**
 * Calls {@code WebUI.attachHandler(String, HttpServlet, String)} from Java.
 * Scala cannot: its pickled signatures of the {@code attachHandler} overloads
 * name {@code org.eclipse.jetty}, which Spark's published jars relocate to
 * {@code org.sparkproject.jetty}, so overload resolution fails on a missing
 * symbol. javac reads the bytecode, where the types are the relocated ones.
 */
final class UiHandlers {
    private UiHandlers() {}

    static void attachServlet(SparkUI ui, String contextPath, HttpServlet servlet,
            String pathSpec) {
        ui.attachHandler(contextPath, servlet, pathSpec);
    }
}
