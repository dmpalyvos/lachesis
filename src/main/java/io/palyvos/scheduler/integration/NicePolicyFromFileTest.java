package io.palyvos.scheduler.integration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.palyvos.scheduler.adapters.OsAdapter;
import io.palyvos.scheduler.adapters.linux.LinuxAdapter;
import io.palyvos.scheduler.policy.translators.concrete.ConcretePolicyTranslator;
import io.palyvos.scheduler.policy.translators.concrete.NicePolicyTranslator;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.util.SchedulerContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.Validate;

public class NicePolicyFromFileTest {

  public static void main(String[] args) throws IOException {
    Config config = new Config();
    JCommander.newBuilder().addObject(config).build().parse(args);
    SchedulerContext.initSpeProcessInfo(config.jvmPid);
    SchedulerContext.switchToSpeProcessContext();
    BufferedReader reader = Files.newBufferedReader(Paths.get(config.policyPath));
    Map<Subtask, Double> policy = new HashMap<>();
    reader.lines().forEach(line -> {
      String[] values = line.split(",");
      Validate.isTrue(values.length == 2);
      final String id = values[0].trim();
      policy.put(new Subtask(id, id, 0), Double.valueOf(values[1].trim()));
    });
    OsAdapter osAdapter = new LinuxAdapter();
    final Map<ExternalThread, Double> schedule = new HashMap<>();
    osAdapter.jvmThreads(config.jvmPid)
        .forEach(thread -> schedule.put(thread, policy.get(thread.name())));
    ConcretePolicyTranslator translator = NicePolicyTranslator
        .withLinearNormalizer(config.max, config.min);
    translator.applyPolicy(schedule);
  }

  static class Config {

    @Parameter(names = "--policyPath", required = true)
    private String policyPath;

    @Parameter(names = "--jvmPid", required = true)
    private int jvmPid;

    @Parameter(names = "--min")
    private int min = -20;

    @Parameter(names = "--max")
    private int max = 10;
  }
}
