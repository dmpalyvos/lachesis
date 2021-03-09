package io.palyvos.scheduler.adapters;

import io.palyvos.scheduler.task.TaskIndex;

public interface SpeAdapter {

  void updateState();

  TaskIndex taskIndex();

  SpeRuntimeInfo runtimeInfo();

}
