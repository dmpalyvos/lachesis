package io.palyvos.scheduler.adapters.flink;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import io.palyvos.scheduler.task.Task;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

class FlinkPlan {

  @SerializedName("jid")
  @Expose
  public String jid;

  @SerializedName("name")
  @Expose
  public String name;

  @SerializedName("nodes")
  @Expose
  public List<FlinkNode> nodes = new ArrayList<>();

  public Collection<Task> tasks(Collection<FlinkVertex> vertices) {
    Validate.notEmpty(vertices, "No vertices provided!");
    Map<String, Task> tasks = vertices.stream().map(vertex -> vertex.toTask(this.jid))
        .collect(Collectors.toMap(task -> task.internalId(), subtask -> subtask));
    for (FlinkNode node : nodes) {
      List<String> inputIds = node.inputs.stream().map(input -> input.id)
          .collect(Collectors.toList());
      Task task = tasks.get(node.id);
      inputIds.forEach(id -> {
        task.upstream().add(tasks.get(id));
        tasks.get(id).downstream().add(task);
      });
    }
    return new ArrayList<>(tasks.values());
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
        .append("jid", jid)
        .append("name", name)
        .append("nodes", nodes)
        .toString();
  }

}
