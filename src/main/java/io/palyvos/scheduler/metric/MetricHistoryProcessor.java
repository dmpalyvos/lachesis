package io.palyvos.scheduler.metric;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.commons.lang3.Validate;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Helper class that maintains sliding time-windows of selected metrics and applies user-defined
 * functions to these windows. The class maintains a {@link SortedMap} of (timestamp, (key, value))
 * for all registered metrics, which is the input to the windowSize functions.
 *
 * @param <T> The type of {@link Metric}s to be recorded.
 */
public class MetricHistoryProcessor<T extends Metric> {

  private static final Logger LOG = LogManager.getLogger();

  private final long windowSize;
  // Metric, Timestamp, Key, Value
  private final Map<T, TreeMap<Long, Map<String, Double>>> history = new HashMap<>();
  private final Map<T, Function<SortedMap<Long, Map<String, Double>>, Map<String, Double>>> metricFunctions;

  /**
   * Construct.
   *
   * @param windowSize      The size of the windowSize, in arbitrary units. Only requirement is that
   *                        the unit is the same as the one used for timestamps in other calls to
   *                        this class.
   * @param metricFunctions A mapping of metrics to a windowSize function, which takes a {@link
   *                        SortedMap} of (timestamp, (key, value)) and produces new metric values
   *                        in the form of a (key, value) map.
   */
  public MetricHistoryProcessor(long windowSize,
      Map<T, Function<SortedMap<Long, Map<String, Double>>, Map<String, Double>>> metricFunctions) {
    Validate.isTrue(windowSize > 0, "windowSize == 0");
    Validate.notEmpty(metricFunctions, "At least one metric required!");
    this.windowSize = windowSize;
    this.metricFunctions = metricFunctions;
    initHistory(metricFunctions.keySet());
  }

  /**
   * Convenience constructor that uses the same windowFunction for all registered metrics.
   */
  public MetricHistoryProcessor(long windowSize,
      Function<SortedMap<Long, Map<String, Double>>, Map<String, Double>> windowFunction,
      T... metrics) {
    Validate.isTrue(windowSize > 0, "windowSize == 0");
    Validate.notEmpty(metrics, "At least one metric required!");
    Validate.notNull(windowFunction, "windowFunction");
    this.windowSize = windowSize;
    this.metricFunctions = new HashMap<>();
    for (T metric : metrics) {
      metricFunctions.put(metric, windowFunction);
    }
    initHistory(Arrays.asList(metrics));
  }

  private void initHistory(Iterable<T> metrics) {
    for (T metric : metrics) {
      history.put(metric, new TreeMap<>());
    }
  }

  public void add(T metric, Map<String, Double> metricValues, long timestamp) {
    Validate.notNull(metricValues, "metricValues");
    Object old = metricHistory(metric).put(timestamp, new HashMap<>(metricValues));
    Validate.validState(old == null,
        "Tried to set %s history for timestamp %d but it was already set!",
        metric, timestamp);
  }

  private TreeMap<Long, Map<String, Double>> metricHistory(T metric) {
    Validate.notNull(metric, "metric");
    Validate.isTrue(metricFunctions.keySet().contains(metric),
        "Not configured to record history for metric '%s'", metric);
    return history.get(metric);
  }

  public Map<String, Double> process(T metric, long timestamp) {
    TreeMap<Long, Map<String, Double>> metricHistory = metricHistory(metric);
    if (metricHistory.isEmpty()) {
      return Collections.emptyMap();
    }
    if (metricHistory.size() == 1) {
      LOG.warn("Only one value in metric history for metric {}", metric);
      Map<String, Double> result = metricHistory.firstEntry().getValue();
      return result != null ? result : Collections.emptyMap();
    }
    SortedMap<Long, Map<String, Double>> historyWindow = historyWindow(metric, metricHistory,
        timestamp);
    return metricFunctions.get(metric).apply(historyWindow);

  }

  private SortedMap<Long, Map<String, Double>> historyWindow(T metric,
      TreeMap<Long, Map<String, Double>> metricHistory, long timestamp) {
    Validate.validState(metricHistory.size() > 1,
        "Cannot create a metric windowSize of one value!");
    long rightKey = metricHistory.floorKey(timestamp);
    long leftKey = metricHistory.ceilingKey(rightKey - windowSize);
    LOG.info("{} window of size {} ({} values)",
        metric, rightKey - leftKey, metricHistory.size());
    return metricHistory.subMap(leftKey, true, rightKey, true);

  }

  public void cleanup(long timestamp) {
    for (T metric : metricFunctions.keySet()) {
      cleanup(metric, timestamp);
    }
  }

  public void cleanup(T metric, long timestamp) {
    TreeMap<Long, Map<String, Double>> metricHistory = metricHistory(metric);
    if (metricHistory.size() < 2) {
      return;
    }
    long rightKey = metricHistory.floorKey(timestamp);
    long leftKey = metricHistory.ceilingKey(rightKey - windowSize);
    metricHistory.subMap(Long.MIN_VALUE, leftKey - 1).clear();
    LOG.debug("{} history range (after cleanup): [{}, {}] ({} values)",
        metric,
        metricHistory.firstKey(),
        metricHistory.lastKey(),
        metricHistory.size());
  }

  public enum WindowLatestMinusEarliestFunction implements
      Function<SortedMap<Long, Map<String, Double>>, Map<String, Double>> {
    INSTANCE;

    @Override
    public Map<String, Double> apply(SortedMap<Long, Map<String, Double>> historyWindow) {
      Map<String, Double> latest = historyWindow.get(historyWindow.lastKey());
      Map<String, Double> earliest = historyWindow.get(historyWindow.firstKey());
      Map<String, Double> latestMinEarliest = new HashMap<>();
      for (String key : latest.keySet()) {
        Double latestValue = latest.get(key);
        Double earliestValue = earliest.get(key);
        Double result =
            (latestValue != null && earliestValue != null) ? latestValue - earliestValue : 0;
        latestMinEarliest.put(key, result);
      }
      return latestMinEarliest;
    }
  }

  public enum WindowLinearRegressionFunction implements Function<SortedMap<Long, Map<String, Double>>, Map<String, Double>> {
    //FIXME: Incomplete implementation
    INSTANCE;

    @Override
    public Map<String, Double> apply(SortedMap<Long, Map<String, Double>> historyWindow) {
      Map<String, SimpleRegression> regressions = new HashMap<>();
      Long lastKey = historyWindow.lastKey();
      Set<String> latest = historyWindow.get(lastKey).keySet();
      latest.forEach(key -> regressions.computeIfAbsent(key, k -> new SimpleRegression()));
      for (Long ts : historyWindow.keySet()) {
        Map<String, Double> metricValues = historyWindow.get(ts);
        for (String entity : metricValues.keySet()) {
          SimpleRegression regression = regressions.get(entity);
          if (regression == null) {
            //FIXME
            continue;
          }
          regression.addData(ts, metricValues.get(entity));
        }
      }
      Map<String, Double> result = new HashMap<>();
      for (Entry<String, SimpleRegression> entry : regressions.entrySet()) {
        String k = entry.getKey();
        SimpleRegression r = entry.getValue();
        r.regress();
        result.put(k, r.predict(lastKey) - r.predict(historyWindow.subMap(historyWindow.firstKey(), lastKey).lastKey()));
      }
      return result;
    }
  }

}
