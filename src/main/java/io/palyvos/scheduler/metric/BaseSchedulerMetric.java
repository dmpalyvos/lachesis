package io.palyvos.scheduler.metric;

import io.palyvos.scheduler.task.Subtask;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskGraphTraverser;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public enum BaseSchedulerMetric implements SchedulerMetric {
  /**
   * Relatively long time compared to query runtime
   */
  SUBTASK_TUPLES_IN_TOTAL,
  SUBTASK_TUPLES_OUT_TOTAL,
  /**
   * Recent compared to query runtime
   */
  SUBTASK_TUPLES_IN_RECENT,
  SUBTASK_TUPLES_OUT_RECENT,
  TASK_QUEUE_SIZE_FROM_SUBTASK_DATA(true, SUBTASK_TUPLES_IN_TOTAL, SUBTASK_TUPLES_OUT_TOTAL) {
    @Override
    public void compute(SchedulerMetricProvider provider) {
      computeTaskQueueSize(this, SUBTASK_TUPLES_IN_TOTAL, SUBTASK_TUPLES_OUT_TOTAL, provider);
    }
  },
  TASK_QUEUE_SIZE_RECENT_FROM_SUBTASK_DATA(true, SUBTASK_TUPLES_IN_RECENT,
      SUBTASK_TUPLES_OUT_RECENT) {
    @Override
    public void compute(SchedulerMetricProvider provider) {
      computeTaskQueueSize(this, SUBTASK_TUPLES_IN_RECENT, SUBTASK_TUPLES_OUT_RECENT, provider);
    }
  },
  SUBTASK_SELECTIVITY(true, SUBTASK_TUPLES_IN_RECENT, SUBTASK_TUPLES_OUT_RECENT) {
    @Override
    public void compute(SchedulerMetricProvider provider) {
      final Map<String, Double> subtaskIn = provider.get(SUBTASK_TUPLES_IN_RECENT);
      final Map<String, Double> subtaskOut = provider.get(SUBTASK_TUPLES_OUT_RECENT);
      final Map<String, Double> selectivity = new HashMap<>();
      for (Subtask subtask : provider.taskIndex().subtasks()) {
        Double in = subtaskIn.get(subtask.id());
        Double out = subtaskOut.get(subtask.id());
        if (in == null || out == null
            || in == 0 && provider.taskGraphTraverser().sourceSubtasks().contains(subtask)
            || out == 0 && provider.taskGraphTraverser().sinkSubtasks().contains(subtask)) {
          LOG.debug(
              "Manually setting selectivity to 1.0 for subtask {}. TUPLES_IN = {}, TUPLES_OUT = {}",
              subtask, in, out);
          selectivity.put(subtask.id(), 1.0);
          continue;
        }
        selectivity.put(subtask.id(), out / in);
      }
      provider.replaceMetricValues(this, selectivity);
    }
  },
  THREAD_CPU_UTILIZATION,
  SUBTASK_CPU_UTILIZATION(true, THREAD_CPU_UTILIZATION) {
    @Override
    public void compute(SchedulerMetricProvider provider) {
      final Map<String, Double> threadUtilization = provider.get(THREAD_CPU_UTILIZATION);
      Map<String, Double> utilization = new HashMap<>();
      for (Subtask subtask : provider.taskIndex().subtasks()) {
        // Sum utilization for all threads of the subtask
        List<Double> subtaskThreadsUtilization = provider.taskIndex().pids(subtask.id()).stream()
            .map(pid -> threadUtilization.get(String.valueOf(pid))).filter(u -> u != null)
            .collect(Collectors.toList());
        Validate.validState(!subtaskThreadsUtilization.isEmpty(),
            "No CPU Utilization info for subtask %s", subtask);
        double subtaskTotalUtilization = subtaskThreadsUtilization.stream().mapToDouble(v -> v)
            .sum();
        utilization.put(subtask.id(), subtaskTotalUtilization);
      }
      provider.replaceMetricValues(this, utilization);
    }
  },
  SUBTASK_COST(true, SUBTASK_CPU_UTILIZATION, SUBTASK_TUPLES_IN_RECENT, SUBTASK_TUPLES_OUT_RECENT) {
    @Override
    public void compute(SchedulerMetricProvider provider) {
      final Map<String, Double> utilization = provider.get(SUBTASK_CPU_UTILIZATION);
      final Map<String, Double> subtaskIn = provider.get(SUBTASK_TUPLES_IN_RECENT);
      final Map<String, Double> subtaskOut = provider.get(SUBTASK_TUPLES_OUT_RECENT);
      final Map<String, Double> cost = new HashMap<>();
      for (Subtask subtask : provider.taskIndex().subtasks()) {
        Double in = subtaskIn.get(subtask.id());
        Double out = subtaskOut.get(subtask.id());
        Double subtaskUtilization = utilization.get(subtask.id());
        Validate.notNull(subtaskUtilization, "Utilization for subtask %s missing!", subtask.id());
        double subtaskCost = subtaskUtilization / processedTuplesNonNull(in, out);
        cost.put(subtask.id(), Double.isFinite(subtaskCost) ? subtaskCost : Double.NaN);
      }
      provider.replaceMetricValues(this, cost);
    }

    private double processedTuplesNonNull(Double in, Double out) {
      if (in != null) {
        return in;
      }
      return (out != null) ? out : 0.0;
    }
  },
  /**
   * output_tuples/input_tuple
   */
  SUBTASK_GLOBAL_SELECTIVITY(true, SUBTASK_SELECTIVITY) {
    @Override
    public void compute(SchedulerMetricProvider provider) {
      final Map<String, Double> globalSelectivity = new HashMap<>();
      final Map<String, Double> selectivity = provider.get(SUBTASK_SELECTIVITY);
      final TaskGraphTraverser traverser = provider.taskGraphTraverser();
      traverser.forEachSubtaskFromSinkBFS(subtask -> {
        double subtaskGlobalSelectivity =
            downstreamSelectivity(subtask, globalSelectivity, provider) *
                selectivity.get(subtask.id());
        globalSelectivity.put(subtask.id(), subtaskGlobalSelectivity);
      });
      provider.replaceMetricValues(this, globalSelectivity);
    }

    private double downstreamSelectivity(Subtask subtask, Map<String, Double> globalSelectivity,
        SchedulerMetricProvider provider) {
      final List<Double> downstreamSelectivities = provider.taskIndex()
          .downstream(subtask).stream().map(st -> globalSelectivity.get(st.id()))
          .collect(Collectors.toList());
      if (downstreamSelectivities.isEmpty()) {
        return 1;
      }
      if (downstreamSelectivities.size() == 1) {
        return downstreamSelectivities.get(0);
      }
      double downstreamSelectivitySum = downstreamSelectivities.stream().mapToDouble(v -> v)
          .sum();
      double downstreamSelectivityMult = downstreamSelectivities.stream().mapToDouble(v -> v)
          .reduce((a, b) -> a * b).orElse(0);
      return downstreamSelectivitySum - downstreamSelectivityMult;
    }
  },
  /**
   * cost/input_tuple
   */
  SUBTASK_GLOBAL_AVERAGE_COST(true, SUBTASK_COST, SUBTASK_SELECTIVITY) {
    @Override
    public void compute(SchedulerMetricProvider provider) {
      final Map<String, Double> globalCost = new HashMap<>();
      final Map<String, Double> selectivity = provider.get(SUBTASK_SELECTIVITY);
      final Map<String, Double> costs = provider.get(SUBTASK_COST);
      final TaskGraphTraverser traverser = provider.taskGraphTraverser();
      traverser.forEachSubtaskFromSinkBFS(subtask -> {
        final Collection<Subtask> downstream = provider.taskIndex().downstream(subtask);
        double subtaskSelectivity = selectivity.get(subtask.id());
        double subtaskGlobalCost = costs.get(subtask.id());
        for (Subtask d : downstream) {
          subtaskGlobalCost += subtaskSelectivity * globalCost.get(d.id());
        }
        globalCost.put(subtask.id(), subtaskGlobalCost);
      });
      provider.replaceMetricValues(this, globalCost);
    }
  },
  /**
   * output_tuples/cost
   */
  SUBTASK_GLOBAL_RATE(true, SUBTASK_GLOBAL_SELECTIVITY, SUBTASK_GLOBAL_AVERAGE_COST) {
    @Override
    public void compute(SchedulerMetricProvider provider) {
      final Map<String, Double> globalRates = new HashMap<>();
      final Map<String, Double> globalSelectivity = provider.get(SUBTASK_GLOBAL_SELECTIVITY);
      final Map<String, Double> globalCosts = provider.get(SUBTASK_GLOBAL_AVERAGE_COST);
      for (Subtask subtask : provider.taskIndex().subtasks()) {
        double subtaskGlobalCost = globalCosts.get(subtask.id());
        double subtaskGlobalSelectivity = globalSelectivity.get(subtask.id());
        double subtaskGlobalRate = subtaskGlobalSelectivity / subtaskGlobalCost;
        globalRates.put(subtask.id(), subtaskGlobalRate);
      }
      provider.replaceMetricValues(this, globalRates);
    }
  };

  private static final Logger LOG = LogManager.getLogger(BaseSchedulerMetric.class);
  protected final Set<SchedulerMetric> dependencies;
  protected final boolean internal;

  BaseSchedulerMetric() {
    this(false);
  }

  BaseSchedulerMetric(boolean internal) {
    this(internal, new SchedulerMetric[0]);
  }

  BaseSchedulerMetric(boolean internal, SchedulerMetric... dependencies) {
    Validate.notNull(dependencies, "components");
    this.dependencies = new HashSet<>(Arrays.asList(dependencies));
    this.internal = internal;
  }

  @Override
  public void compute(SchedulerMetricProvider provider) {
    LOG.trace("No computation for {}", this);
  }

  @Override
  public final boolean isInternal() {
    return internal;
  }

  @Override
  public Set<SchedulerMetric> dependencies() {
    return dependencies;
  }

  protected final void computeTaskQueueSize(BaseSchedulerMetric metric,
      BaseSchedulerMetric inTuplesMetric, BaseSchedulerMetric outTuplesMetric,
      SchedulerMetricProvider provider) {
    final Map<String, Double> subtaskIn = provider.get(inTuplesMetric);
    final Map<String, Double> subtaskOut = provider.get(outTuplesMetric);
    final Map<String, Double> qs = new HashMap<>();
    for (Task task : provider.taskIndex().tasks()) {
      double writtenUpstream = task.upstream().stream()
          .flatMap(upstreamTask -> upstreamTask.subtasks().stream())
          .map(subtask -> subtaskOut.get(subtask.id())).mapToDouble(v -> v != null ? v : 0)
          .sum();
      double readByTask = task.subtasks().stream()
          .map(subtask -> subtaskIn.get(subtask.id())).mapToDouble(v -> v != null ? v : 0)
          .sum();
      qs.put(task.id(), writtenUpstream - readByTask);
    }
    provider.replaceMetricValues(metric, qs);
  }
}
