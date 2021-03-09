package io.palyvos.scheduler.adapters;

import io.palyvos.scheduler.task.ExternalThread;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public interface OsAdapter {

  List<ExternalThread> retrieveThreads(int pid);

  default List<ExternalThread> retrieveThreads(Collection<Integer> pids) {
    List<ExternalThread> threads = new ArrayList<>();
    for (int pid : pids) {
      threads.addAll(retrieveThreads(pid));
    }
    return Collections.unmodifiableList(threads);
  }
}
