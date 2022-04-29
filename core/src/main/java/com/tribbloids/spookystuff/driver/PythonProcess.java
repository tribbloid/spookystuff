/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.tribbloids.spookystuff.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;

/**
 * direct from https://github.com/apache/zeppelin/blob/master/python/src/main/java/org/apache/zeppelin/python/PythonProcess.java
 * Object encapsulated interactive
 * Python process (REPL) used by python interpreter
 * No need to convert to scala, if it works don't fix it
 */
//TODO: is it thread safe?
//TODO: findPid use a private field of UNIXProcess and may be OS-dependent, need more test on other OS
public class PythonProcess {
  Logger logger = LoggerFactory.getLogger(PythonProcess.class);

  InputStream stdout;
  OutputStream stdin;
  BufferedWriter writer;
  BufferedReader reader;
  protected Process process;

  private String binPath;
  private long pid;

  public PythonProcess(String binPath) {
    this.binPath = binPath;
  }

  public void open() throws IOException {
    ProcessBuilder builder = new ProcessBuilder(binPath, "-iu");

    builder.redirectErrorStream(true);
    process = builder.start();
    stdout = process.getInputStream();
    stdin = process.getOutputStream();
    writer = new BufferedWriter(new OutputStreamWriter(stdin));
    reader = new BufferedReader(new InputStreamReader(stdout));
    try {
      pid = findPid();
    } catch (Exception e) {
      logger.warn("Can't find python pid process", e);
      pid = -1;
    }

    //santity check and drain version info
    String firstRes = sendAndGetResult("print(1+1)");
    String[] lines = firstRes.trim().split("\n");
    assert lines[0].startsWith("Python");
    assert lines[lines.length -1].endsWith("2");
  }

  public void closeProcess() throws IOException {
    try {
      process.destroy();
    }
    catch (Throwable e){
      process.destroyForcibly();
    }
    reader.close();
    writer.close();
    stdin.close();
    stdout.close();
  }

  public void interrupt() throws IOException {
    if (pid > -1) {
      logger.info("Interrupting: Sending SIGINT signal to PID : " + pid);
      Runtime.getRuntime().exec("kill -SIGINT " + pid);
    } else {
      logger.warn("Non UNIX/Linux system, close the interpreter");
      closeProcess();
    }
  }

  //TODO: change to StringBuffer! current impl is not thread safe
  volatile public String outputBuffer = "";
  public String sendAndGetResult(String cmd) throws IOException {

    writer.write(cmd + "\n\n");
    writer.write("print (\"*!?flush reader!?*\")\n\n");
    writer.flush();

    outputBuffer = "";
    String line;

    while (!(line = reader.readLine()).contains("*!?flush reader!?*")) {
      logger.debug(logPyOutput(line));
      if (line.equals("...")) {
        logger.warn("Syntax error ! ");
        outputBuffer += "Syntax error ! ";
        break;
      }
      outputBuffer += "\r" + line + "\n"; //TODO: cause extra empty lines between outputs in Linux, fix it!
    }
    return outputBuffer;
  }

  protected String logPyOutput(String line) {
    return "Read line from python shell : " + line;
  }

  //only use reflection to find UNIXProcess.pid.
  private long findPid() throws NoSuchFieldException, IllegalAccessException {
    long pid = -1;
    if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
      Field f = process.getClass().getDeclaredField("pid");
      f.setAccessible(true);
      pid = f.getLong(process);
      f.setAccessible(false);
    }
    return pid;
  }

  public long getPid() {
    return pid;
  }

}
