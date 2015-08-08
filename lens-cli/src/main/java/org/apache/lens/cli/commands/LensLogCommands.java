/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cli.commands;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

public class LensLogCommands extends BaseLensCommand {

  @CliCommand(value = "shows logs", help = "show logs for a given log file")
  public String getLogs(@CliOption(key = {"", "requestId"}, mandatory = true, help = "<requestId|queryhadnle>")
    String logFile, @CliOption(key = {"save_location"}, mandatory = false, help = "<save_location>") String location) {
    try {
      Response response = getClient().getLogs(logFile);
      if (response.getStatus() == Response.Status.OK.getStatusCode()) {
        if (StringUtils.isBlank(location)) {
          PrintStream outStream = new PrintStream(System.out);
          try (InputStream stream = response.readEntity(InputStream.class);) {
            IOUtils.copy(stream, outStream);
          }
          return "printed complete log content";
        } else {
          location = getValidPath(location, true, true);
          String fileName = logFile;
          location = getValidPath(location + File.separator + fileName, false, false);
          try (InputStream stream = response.readEntity(InputStream.class);
               FileOutputStream outStream = new FileOutputStream(new File(location))) {
            IOUtils.copy(stream, outStream);
          }
          return "Saved to " + location;
        }
      } else {
        return response.toString();
      }
    } catch (Throwable t) {
      return t.getMessage();
    }
  }
}
