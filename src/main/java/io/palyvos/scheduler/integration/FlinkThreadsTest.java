package io.palyvos.scheduler.integration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.palyvos.scheduler.adapters.flink.FlinkAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.Collection;

public class FlinkThreadsTest {

  public static void main(String[] args) {
    Config config = new Config();
    JCommander.newBuilder().addObject(config).build().parse(args);
    SchedulerContext.initSpeProcessInfo(config.jvmPid);
    SchedulerContext.switchToSpeProcessContext();

    Collection<ExternalThread> threads = new LinuxAdapter().jvmThreads(config.jvmPid);

    FlinkAdapter flinkAdapter = new FlinkAdapter(config.jvmPid, "localhost", 8081);
    flinkAdapter.updateTasks();
    System.out.println(flinkAdapter.tasks());
  }

  static class Config {

    @Parameter(names = "--jvmPid", required = true)
    private int jvmPid;

  }
}
