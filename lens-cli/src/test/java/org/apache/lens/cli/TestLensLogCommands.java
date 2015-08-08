package org.apache.lens.cli;

import org.apache.commons.io.FileUtils;
import org.apache.lens.cli.commands.LensConnectionCommands;
import org.apache.lens.client.LensClient;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TestLensLogCommands {

  @Test
  public void testLogsCommand() throws IOException {
    LensClient client = new LensClient();
    LensConnectionCommands commands = new LensConnectionCommands();
    commands.setClient(client);

    String request = "testId";
    File file = createFileWithContent(request, "test log resource");

    // create output directory to store the resulted log file
    String outputDirName = "target/sample-logs/";
    File dir = new File(outputDirName);
    dir.mkdirs();

    String response = commands.getLogs(request, outputDirName);
    File outputFile = new File(outputDirName + request);
    Assert.assertTrue(FileUtils.contentEquals(file, outputFile));
    Assert.assertTrue(response.contains("Saved to"));

    response = commands.getLogs(request, null);
    Assert.assertTrue(response.contains("printed complete log content"));

    // check 404 response
    response = commands.getLogs("random", null);
    Assert.assertTrue(response.contains("404"));
  }

  private File createNewPath(String fileName) {
    File f = new File(fileName);
    try {
      if (!f.exists()) {
        f.createNewFile();
      }
    } catch (IOException e) {
      Assert.fail("Unable to create test file, so bailing out.");
    }
    return f;
  }

  private File createFileWithContent(String filename, String content) throws IOException {
    File file = createNewPath("target/" + filename + ".log");
    FileWriter fw = new FileWriter(file.getAbsoluteFile());
    BufferedWriter bw = new BufferedWriter(fw);
    bw.write(content);
    bw.close();
    return file;
  }
}
