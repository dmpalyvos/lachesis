package io.palyvos.scheduler.util;

import com.beust.jcommander.IStringConverter;
import org.apache.logging.log4j.Level;

public class Log4jLevelConverter implements IStringConverter<Level> {

  @Override
  public Level convert(String s) {
    return Level.toLevel(s);
  }
}
