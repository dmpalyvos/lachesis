package io.palyvos.scheduler.adapters.flink;

import io.palyvos.scheduler.metric.AbstractMetricProvider;
import io.palyvos.scheduler.metric.graphite.GraphiteDataFetcher;
import io.palyvos.scheduler.metric.graphite.GraphiteMetricReport;
import io.palyvos.scheduler.task.Task;
import io.palyvos.scheduler.task.TaskGraphTraverser;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FlinkGraphiteMetricProvider extends AbstractMetricProvider<FlinkGraphiteMetric> {

  private static final Logger LOG = LogManager.getLogger();

  private final GraphiteDataFetcher graphiteDataFetcher;
  final TaskGraphTraverser traverser;

  public FlinkGraphiteMetricProvider(String graphiteHost, int graphitePort, Collection<Task> tasks) {
    super(mappingFor(FlinkGraphiteMetric.values()), FlinkGraphiteMetric.class);
    Validate.notEmpty(tasks, "no tasks given");
    this.graphiteDataFetcher = new GraphiteDataFetcher(graphiteHost, graphitePort);
    this.traverser = new TaskGraphTraverser(tasks);
  }

  @Override
  protected void doCompute(FlinkGraphiteMetric metric) {
    metric.compute(this);
  }

  Map<String, Double> fetchFromGraphite(String target,
      int windowSeconds, Function<GraphiteMetricReport, Double> reduceFunction) {
    return graphiteDataFetcher.fetchFromGraphite(target, windowSeconds, reduceFunction);
  }

}
