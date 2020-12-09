package io.palyvos.scheduler.policy.cgroup;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BasicCGroupActionExecutor implements CGroupActionExecutor {

  private static final Logger LOG = LogManager.getLogger();
  private static final int ENFORCER_THREADS = 4;

  @Override
  public void create(Collection<CGroup> cgroups) {
    SchedulerContext.switchToRootContext();
    final ExecutorService executor = newExecutor();
    final List<Future<Boolean>> futures = new ArrayList<>();
    for (CGroup cgroup : cgroups) {
          futures.add(executor.submit(() -> cgroup.create()));
    }
    wait(futures, true);
    executor.shutdown();
    SchedulerContext.switchToSpeProcessContext();
  }

  @Override
  public void delete(Collection<CGroup> cgroups) {
    SchedulerContext.switchToRootContext();
    final ExecutorService executor = newExecutor();
    final List<Future<Boolean>> futures = new ArrayList<>();
    for (CGroup cgroup : cgroups) {
      futures.add(executor.submit(() -> cgroup.delete()));
    }
    wait(futures, false);
    executor.shutdown();
    SchedulerContext.switchToSpeProcessContext();
  }

  @Override
  public void updateParameters(Map<CGroup, Collection<CGroupParameterContainer>> schedule) {
    SchedulerContext.switchToRootContext();
    final ExecutorService executor = newExecutor();
    final List<Future<Boolean>> futures = new ArrayList<>();
    for (CGroup cgroup : schedule.keySet()) {
      Collection<CGroupParameterContainer> parameters = schedule.get(cgroup);
      parameters.forEach(parameter ->
          futures.add(executor.submit(() -> cgroup.set(parameter.key(), parameter.value())))
      );
    }
    wait(futures, true);
    executor.shutdown();
    SchedulerContext.switchToSpeProcessContext();
  }

  @Override
  public void updateAssignment(Map<CGroup, Collection<ExternalThread>> assignment) {
    SchedulerContext.switchToRootContext();
    final ExecutorService executor = newExecutor();
    final List<Future<Boolean>> futures = new ArrayList<>();
    for (CGroup cgroup : assignment.keySet()) {
      futures.add(executor.submit(() -> cgroup.classify(assignment.get(cgroup))));
    }
    wait(futures, true);
    executor.shutdown();
    SchedulerContext.switchToSpeProcessContext();
  }

  private ExecutorService newExecutor() {
    return Executors.newFixedThreadPool(ENFORCER_THREADS);
  }

  private void wait(List<Future<Boolean>> futures, boolean checkSuccess) {
    for (Future<Boolean> future : futures) {
      try {
        final boolean success = future.get();
        Validate.validState(!checkSuccess || success, "Execution failure!");
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for policy to be applied");
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        LOG.error("Policy application failed!");
        throw new RuntimeException(e);
      }
    }
  }

}
