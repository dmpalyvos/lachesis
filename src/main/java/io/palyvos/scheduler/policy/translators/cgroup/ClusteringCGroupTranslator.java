package io.palyvos.scheduler.policy.translators.cgroup;

import static io.palyvos.scheduler.util.cgroup.CGController.CPU;

import io.palyvos.scheduler.task.CGroup;
import io.palyvos.scheduler.task.CGroupParameterContainer;
import io.palyvos.scheduler.task.ExternalThread;
import io.palyvos.scheduler.task.Task;
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
  private final CGroupPolicyTranslator translator = new BasicCGroupPolicyTranslator();

  public ClusteringCGroupTranslator(int ngroups, Collection<Task> tasks) {
    this.ngroups = ngroups;
    this.tasks = tasks;
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
    LOG.info("Retrieved {} clusters", clusters.size());
    int index = 0;
    Map<String, CGroup> rawAssignment = new HashMap<>();
    Map<CGroup, Collection<ExternalThread>> assignment = new HashMap<>();
    Map<CGroup, Double> cgroupValues = new HashMap<>();
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
    translator.create(cgroupValues.keySet());
    translator.updateAssignment(assignment);
    translator.updateParameters(scheduleFunction.apply(cgroupValues));
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
