package io.palyvos.scheduler.policy.translators.cgroup;

import static io.palyvos.scheduler.util.cgroup.CGController.CPU;

import io.palyvos.scheduler.task.CGroup;
import io.palyvos.scheduler.task.CGroupParameterContainer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.util.SchedulerContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusteringCGroupTranslator {

  private static final Logger LOG = LogManager.getLogger();

  private static final CGroup PARENT_CGROUP = new CGroup("/lachesis", CPU);
  private final int ngroups;
  private final Collection<Task> tasks;
  private final CGroupPolicyTranslator policyTranslator;
  private final CGroupScheduleGraphiteReporter graphiteReporter = new CGroupScheduleGraphiteReporter(
      SchedulerContext.GRAPHITE_STATS_HOST, SchedulerContext.GRAPHITE_STATS_PORT);

  public ClusteringCGroupTranslator(int ngroups, Collection<Task> tasks,
      CGroupPolicyTranslator policyTranslator) {
    this.ngroups = ngroups;
    this.tasks = tasks;
    this.policyTranslator = policyTranslator;
  }

  public ClusteringCGroupTranslator(int ngroups, Collection<Task> tasks) {
    this(ngroups, tasks, new BasicCGroupPolicyTranslator());
  }


  public void schedule(Map<String, Double> metricValues,
      Function<Map<CGroup, Double>, Map<CGroup, Collection<CGroupParameterContainer>>> scheduleFunction) {
    List<ClusterableMetricValue> values = new ArrayList<>();
    metricValues.forEach((k, v) -> {
      if (Double.isFinite(v)) {
        values.add(new ClusterableMetricValue(k, v));
      }
    });
    KMeansPlusPlusClusterer<ClusterableMetricValue> clusterer = new KMeansPlusPlusClusterer<>(
        ngroups, 100);
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
    policyTranslator.create(cgroupValues.keySet());
    policyTranslator.updateAssignment(assignment);
    Map<CGroup, Collection<CGroupParameterContainer>> schedule = scheduleFunction
        .apply(cgroupValues);
    policyTranslator.updateParameters(schedule);
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
