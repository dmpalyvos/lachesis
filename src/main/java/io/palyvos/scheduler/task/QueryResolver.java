/*
 * Copyright 2018-2019
 *     Dimitris Palyvos-Giannas
 *     Vincenzo Gulisano
 *     Marina Papatriantafilou
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contact:
 *     Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package io.palyvos.scheduler.task;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryResolver {

  private static final Logger LOG = LogManager.getLogger();
  private final Map<Integer, Query> queryByIndex = new HashMap<>();
  private final Map<Task, Query> queryOfTask = new HashMap<>();
  private int nQueries;

  public QueryResolver(Collection<Task> tasks) {
    resolveQueries(tasks);
  }

  public Collection<Query> queries() {
    return Collections.unmodifiableCollection(queryByIndex.values());
  }

  private void resolveQueries(Collection<Task> tasks) {
    for (Task task : tasks) {
      traverseTaskGraph(task);
    }
    LOG.info("{} queries found", nQueries);
    for (int queryNumber : queryByIndex.keySet()) {
      LOG.trace("Query #{} -> {}", queryNumber, queryByIndex.get(queryNumber));
    }
  }

  private void traverseTaskGraph(Task task) {
    if (queryOfTask.get(task) != null) { // Visited
      return;
    }
    nQueries += 1;
    final int currentQueryIndex = nQueries;
    final Query query = new Query(currentQueryIndex);
    queryByIndex.put(currentQueryIndex, query);

    final Deque<Task> q = new ArrayDeque<>();
    q.addLast(task);
    while (!q.isEmpty()) {
      Task current = q.removeFirst();
      if (queryOfTask.get(current) == null) { // Not visited
        // Update both representations
        queryByIndex.get(currentQueryIndex).tasks().add(current);
        queryOfTask.put(current, query);
        // Recursively for the rest of the graph
        if (current.downstream().isEmpty()) {
          query.sinks().add(current);
        }
        if (current.upstream().isEmpty()) {
          query.sources().add(current);
          query.setName(current.id());
        }
        current.downstream().forEach(t -> q.addLast(t));
        current.upstream().forEach(t -> q.addLast(t));
      }
    }
  }

}
