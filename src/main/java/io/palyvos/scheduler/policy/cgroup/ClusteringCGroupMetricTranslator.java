package io.palyvos.scheduler.policy.cgroup;

import static io.palyvos.scheduler.policy.cgroup.CGroupController.CPU;

import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusteringCGroupMetricTranslator implements CGroupMetricTranslator {

  public static final String NAME = "CLUSTERING";
  private static final Logger LOG = LogManager.getLogger();

  private static final CGroup PARENT_CGROUP = new CGroup("/lachesis", CPU);
  private static final int K_MEANS_MAX_ITERATIONS = 100;
  private final int ngroups;
  private final CGroupActionExecutor cgroupActionExecutor;
  private final CGroupScheduleGraphiteReporter graphiteReporter = new CGroupScheduleGraphiteReporter(
      SchedulerContext.GRAPHITE_STATS_HOST, SchedulerContext.GRAPHITE_STATS_PORT);

  private Collection<Task> tasks;

  public ClusteringCGroupMetricTranslator(int ngroups, CGroupActionExecutor cgroupActionExecutor) {
    Validate.isTrue(ngroups > 0, "ngroups <= 0");
    Validate.notNull(cgroupActionExecutor, "cgroupActionExecutor");
    this.ngroups = ngroups;
    this.cgroupActionExecutor = cgroupActionExecutor;
  }

  public ClusteringCGroupMetricTranslator(int ngroups) {
    this(ngroups, new BasicCGroupActionExecutor());
  }

  @Override
  public void init(Collection<Task> tasks) {
    this.tasks = tasks;
  }


  @Override
  public void apply(Map<String, Double> metricValues,
      CGroupPriorityToParametersFunction priorityToParametersFunction) {
    List<ClusterableMetricValue> values = new ArrayList<>();
    metricValues.forEach((k, v) -> {
      if (Double.isFinite(v)) {
        values.add(new ClusterableMetricValue(k, v));
      }
    });
    KMeansPlusPlusClusterer<ClusterableMetricValue> clusterer = new KMeansPlusPlusClusterer<>(
        ngroups, K_MEANS_MAX_ITERATIONS);
    List<CentroidCluster<ClusterableMetricValue>> clusters = clusterer.cluster(values);

    LOG.info("Found {} clusters", clusters.size());

    Map<String, CGroup> rawAssignment = new HashMap<>();
    Map<CGroup, Collection<ExternalThread>> assignment = new HashMap<>();
    Map<CGroup, Double> cgroupValues = new HashMap<>();

    int index = 0;
    for (CentroidCluster<ClusterableMetricValue> cluster : clusters) {
      Clusterable center = cluster.getCenter();
      List<ClusterableMetricValue> points = cluster.getPoints();
      CGroup cgroup = PARENT_CGROUP.newChild(String.valueOf(index++));
      points.stream().map(v -> v.id).forEach(id -> rawAssignment.put(id, cgroup));
      cgroupValues.put(cgroup, center.getPoint()[0]);
    }
    for (Task task : tasks) {
      CGroup cgroup = rawAssignment.get(task.id());
      if (cgroup == null) {
        continue;
      }
      assignment.computeIfAbsent(cgroup, c -> new ArrayList<>()).addAll(task.threads());
    }
    cgroupActionExecutor.create(cgroupValues.keySet());
    cgroupActionExecutor.updateAssignment(assignment);
    Map<CGroup, Collection<CGroupParameterContainer>> schedule = priorityToParametersFunction
        .apply(cgroupValues);
    cgroupActionExecutor.updateParameters(schedule);
    graphiteReporter.report(cgroupValues, schedule);
  }


  private static class ClusterableMetricValue implements Clusterable {

    private final String id;
    private final double[] point;

    public ClusterableMetricValue(String id, double value) {
      this.id = id;
      this.point = new double[]{value};
    }

    @Override
    public double[] getPoint() {
      return point;
    }
  }
}
