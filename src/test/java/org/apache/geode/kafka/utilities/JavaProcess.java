/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.kafka.utilities;

import java.io.File;
import java.io.IOException;

public class JavaProcess {

  public Process process;
  Class classWithMain;

  public JavaProcess(Class classWithMain) {
    this.classWithMain = classWithMain;
  }

  public void exec(String... args) throws IOException, InterruptedException {
    String java =
        System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    String className = classWithMain.getName();
    int commandLength = args.length + 4;
    String[] processBuilderCommand = new String[commandLength];
    processBuilderCommand[0] = java;
    processBuilderCommand[1] = "-cp";
    processBuilderCommand[2] = classpath;
    processBuilderCommand[3] = className;
    System.arraycopy(args, 0, processBuilderCommand, 4, args.length);
    ProcessBuilder builder = new ProcessBuilder(
        processBuilderCommand);

    process = builder.inheritIO().start();
  }

  public void waitFor() throws InterruptedException {
    process.waitFor();
  }

  public void destroy() {
    process.destroy();
  }



}
