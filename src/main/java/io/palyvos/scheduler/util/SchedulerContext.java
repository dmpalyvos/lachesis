package io.palyvos.scheduler.util;

import io.palyvos.scheduler.util.command.ExternalCommand;
import io.palyvos.scheduler.util.command.ExternalCommandRunner.CommandResult;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SchedulerContext {

  public static final CharSequence SCHEDULER_NAME = "lachesis";
  private static final Logger LOG = LogManager.getLogger(SchedulerContext.class);

  private static final UserInfo ROOT_USER_INFO = new UserInfo("root", 0, "root", 0);
  private static UserInfo SPE_PROCESS_USER_INFO;

  // Global configuration parameters
  public static boolean IS_DISTRIBUTED = false;
  public static Function<String, String> THREAD_NAME_GRAPHITE_CONVERTER = s -> s;
  public static int METRIC_RECENT_PERIOD_SECONDS = 10;
  public static int METRIC_TOTAL_PERIOD_SECONDS = 600;
  public static String STATISTICS_FOLDER = ".";
  public static boolean STATISTICS_AUTO_FLUSH = true;
  public static String GRAPHITE_STATS_HOST = "129.16.20.158";
  public static int GRAPHITE_STATS_PORT = 2003;
  // Total cpu shares assigned to the root /lachesis cgroup
  // Needs to be much higher than the cpu shares of other root cgroups to avoid starving the SPE tasks
  public static final long TOTAL_CPU_SHARES = 40000;
  // Number of threads that apply the external commands
  public static final int SINGLE_PRIO_ENFORCER_THREADS = 8;
  public static final int CGROUP_ENFORCER_THREADS = 8;

  static {
    ToStringBuilder.setDefaultStyle(ToStringStyle.NO_CLASS_NAME_STYLE);
    Thread.setDefaultUncaughtExceptionHandler((thread, e) -> {
      LOG.warn("Scheduler error", e);
      System.exit(1);
    });
  }

  public static void initSpeProcessInfo(int pid) {
    SPE_PROCESS_USER_INFO = new UIDGIDCommand(pid).getIds();
    LOG.info("Retrieved context for process {}: {}", pid, SPE_PROCESS_USER_INFO);
  }

  public static void switchToSpeProcessContext() {
    switchContext(SPE_PROCESS_USER_INFO);
  }

  public static void switchToRootContext() {
    switchContext(ROOT_USER_INFO);
  }

  private static void switchContext(UserInfo userInfo) {
    LOG.debug("Switching context to {}", userInfo);
    int gidStatus = CLibrary.INSTANCE.setegid(userInfo.gid);
    Validate.validState(gidStatus == 0, "Failed to switch gid!");
    int uidStatus = CLibrary.INSTANCE.seteuid(userInfo.uid);
    Validate.validState(uidStatus == 0, "Failed to switch uid!");
  }


  private SchedulerContext() {

  }

  private static class UIDGIDCommand implements ExternalCommand {

    private static final String EXECUTABLE = "ps";
    private static final String PID_FLAG = "-p";
    private static final String OUTPUT_FLAG = "-o";
    private static final String OUTPUT_COLUMNS = "user=,uid=,group=,gid=";
    private final int pid;
    private static final Pattern PS_OUTPUT_PATTERN = Pattern
        .compile("s*(\\w+)\\s+(\\d+)\\s+(\\w+)\\s+(\\d+)s*");

    public UIDGIDCommand(int pid) {
      Validate.isTrue(pid > 1, "invalid pid: %d", pid);
      this.pid = pid;
    }

    @Override
    public List<String> rawCommand() {
      return Arrays.asList(
          executable(),
          PID_FLAG,
          String.valueOf(pid),
          OUTPUT_FLAG,
          OUTPUT_COLUMNS);
    }

    @Override
    public String executable() {
      return EXECUTABLE;
    }

    UserInfo getIds() {
      CommandResult processResult = call();
      if (processResult.exitCode != 0 || processResult.stdout.isEmpty()) {
        throw new IllegalStateException(String.format("Failed to get info for pid: %d", pid));
      }
      Validate
          .isTrue(processResult.stdout.size() == 1, "%s returned invalid result: %s", executable(),
              processResult.stdoutAsString());
      final String output = processResult.stdout.get(0);
      Matcher matcher = PS_OUTPUT_PATTERN.matcher(output);
      if (!matcher.matches()) {
        throw new IllegalStateException(String.format("Failed to parse ps output: %s", output));
      }
      return new UserInfo(matcher.group(1), Integer.valueOf(matcher.group(2)), matcher.group(3),
          Integer.valueOf(matcher.group(4)));
    }
  }

  private static class UserInfo {

    public final String name;
    public final int uid;
    public final String group;
    public final int gid;

    public UserInfo(String name, int uid, String group, int gid) {
      Validate.notBlank(name, "Blank username!");
      Validate.isTrue(uid >= 0, "Negative uid!");
      Validate.notBlank(group, "Blank group!");
      Validate.isTrue(gid >= 0, "Negative gid!");
      this.name = name;
      this.uid = uid;
      this.group = group;
      this.gid = gid;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("name", name)
          .append("uid", uid)
          .append("group", group)
          .append("gid", gid)
          .toString();
    }
  }


}
