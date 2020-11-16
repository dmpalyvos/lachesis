package io.palyvos.scheduler.adapters.flink;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

class FlinkJobMetrics {
  @SerializedName("read-bytes")
  @Expose
  public long readBytes;

  @SerializedName("read-bytes-complete")
  @Expose
  public boolean readBytesComplete;

  @SerializedName("write-bytes")
  @Expose
  public long writeBytes;

  @SerializedName("write-bytes-complete")
  @Expose
  public boolean writeBytesComplete;

  @SerializedName("read-records")
  @Expose
  public long readRecords;

  @SerializedName("read-records-complete")
  @Expose
  public boolean readRecordsComplete;

  @SerializedName("write-records")
  @Expose
  public long writeRecords;

  @SerializedName("write-records-complete")
  @Expose
  public boolean writeRecordsComplete;

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
        .append("readBytes", readBytes)
        .append("readBytesComplete", readBytesComplete)
        .append("writeBytes", writeBytes)
        .append("writeBytesComplete", writeBytesComplete)
        .append("readRecords", readRecords)
        .append("readRecordsComplete", readRecordsComplete)
        .append("writeRecords", writeRecords)
        .append("writeRecordsComplete", writeRecordsComplete)
        .toString();
  }
}
