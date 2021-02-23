package io.palyvos.scheduler.policy.cgroup;

import static io.palyvos.scheduler.policy.cgroup.CGroupController.CPU;

import io.palyvos.scheduler.metric.SchedulerMetric;
import io.palyvos.scheduler.metric.SchedulerMetricProvider;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
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

public class ClusterinCGroupSchedulingPolicy implements CGroupSchedulingPolicy {

  public static final String NAME = "CLUSTERING";
  private static final Logger LOG = LogManager.getLogger();

  private static final CGroup PARENT_CGROUP = new CGroup("/lachesis", CPU);
  private static final int K_MEANS_MAX_ITERATIONS = 100;
  private final int ngroups;
  private final SchedulerMetric metric;

  public ClusterinCGroupSchedulingPolicy(SchedulerMetric metric, int ngroups) {
    Validate.isTrue(ngroups > 0, "ngroups <= 0");
    Validate.notNull(metric, "metric");
    this.metric = metric;
    this.ngroups = ngroups;
  }

  @Override
  public void init(Collection<Task> tasks, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {
    translator.init(tasks);
    metricProvider.register(metric);
  }

  @Override
  public void apply(Collection<Task> tasks, CGroupTranslator translator,
      SchedulerMetricProvider metricProvider) {

    final List<ClusterableMetricValue> values = new ArrayList<>();
    final Map<String, Double> metricValues = metricProvider.get(metric);
    metricValues.forEach((k, v) -> {
      if (Double.isFinite(v)) {
        values.add(new ClusterableMetricValue(k, v));
      }
    });
    KMeansPlusPlusClusterer<ClusterableMetricValue> clusterer = new KMeansPlusPlusClusterer<>(
        ngroups, K_MEANS_MAX_ITERATIONS);
    List<CentroidCluster<ClusterableMetricValue>> clusters = clusterer.cluster(values);

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
    translator.apply(cgroupValues, assignment);
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
