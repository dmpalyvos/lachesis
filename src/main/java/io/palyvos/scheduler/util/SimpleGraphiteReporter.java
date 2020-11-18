package io.palyvos.scheduler.util;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.URI;

//FIXME: Not tested
public class SimpleGraphiteReporter {

  private final URI graphiteURI;
  private final InetSocketAddress socketAddress;
  private DatagramSocket socket;

  public SimpleGraphiteReporter(String graphiteHost, int graphitePort) throws SocketException {
    this.graphiteURI = URI.create(String.format("http://%s:%d", graphiteHost, graphitePort));
    socketAddress = new InetSocketAddress(graphiteURI.getHost(), graphiteURI.getPort());
    this.socket = new DatagramSocket();
  }


  public void report(long timestampSeconds, String key, Object value) throws IOException {
    byte[] message = String.format("%s %s %d\n", key, value, timestampSeconds).getBytes();
    DatagramPacket packet = new DatagramPacket(message, message.length, socketAddress);
    socket.send(packet);
  }

  private void close() throws IOException {
    socket.close();
  }
}
