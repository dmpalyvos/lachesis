package io.palyvos.scheduler.metric.graphite;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class SimpleGraphiteReporter {

  private static final Pattern GRAPHITE_REJECT_PATTERN = Pattern.compile("[^A-Za-z0-9\\-_>]");
  private final int graphitePort;
  private final String graphiteHost;
  private Socket socket;
  private DataOutputStream output;

  public SimpleGraphiteReporter(String graphiteHost, int graphitePort) {
    this.graphiteHost = graphiteHost;
    this.graphitePort = graphitePort;
  }

  public void open() {
    try {
      socket = new Socket(graphiteHost, graphitePort);
      output = new DataOutputStream(socket.getOutputStream());
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public void report(long timestampSeconds, String key, Object value) throws IOException {
    output.writeBytes(String.format("%s %s %d\n", key, value, timestampSeconds));
  }

  public void close() {
    try {
      output.flush();
      output.close();
      socket.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String cleanGraphiteId(String thread) {
    return GRAPHITE_REJECT_PATTERN.matcher(thread).replaceAll("");
  }
}
