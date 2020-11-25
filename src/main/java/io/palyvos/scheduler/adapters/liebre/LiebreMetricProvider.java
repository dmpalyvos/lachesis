package io.palyvos.scheduler.adapters.liebre;

import io.palyvos.scheduler.metric.AbstractMetricProvider;
import io.palyvos.scheduler.metric.graphite.GraphiteDataFetcher;
import io.palyvos.scheduler.metric.graphite.GraphiteMetricReport;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskGraphTraverser;
import io.palyvos.scheduler.task.TaskIndex;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//FIXME: Builder
public class LiebreMetricProvider extends AbstractMetricProvider<LiebreMetric> {

  private static final Logger LOG = LogManager.getLogger(LiebreMetricProvider.class);

  private final GraphiteDataFetcher graphiteDataFetcher;
  static final String LIEBRE_METRICS_PREFIX = "liebre.*";
  static final int LIEBRE_METRIC_NODE_IDX = 2;
  final TaskGraphTraverser traverser;

  public LiebreMetricProvider(String graphiteHost, int graphitePort, Collection<Task> tasks) {
    super(mappingFor(LiebreMetric.values()), LiebreMetric.class);
    this.graphiteDataFetcher = new GraphiteDataFetcher(graphiteHost, graphitePort);
    Validate.notEmpty(tasks, "no task given");
    traverser = new TaskGraphTraverser(tasks);
  }

  @Override
  protected void doCompute(LiebreMetric metric) {
    metric.compute(this);
  }

  public Map<String, Double> fetchFromGraphite(String target, int windowSeconds,
      Function<GraphiteMetricReport, Double> reduceFunction) {
    return graphiteDataFetcher.fetchFromGraphite(target, windowSeconds, reduceFunction);
  }
}
