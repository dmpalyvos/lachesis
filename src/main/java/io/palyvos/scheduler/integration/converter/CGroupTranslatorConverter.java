package io.palyvos.scheduler.integration.converter;

import com.beust.jcommander.IStringConverter;
import io.palyvos.scheduler.policy.cgroup.CGroupTranslator;
import io.palyvos.scheduler.policy.cgroup.CpuQuotaCGroupTranslator;
import io.palyvos.scheduler.policy.cgroup.CpuSharesCGroupTranslator;

public class CGroupTranslatorConverter implements IStringConverter<CGroupTranslator> {

  public static final int DEFAULT_CPU_PERIOD = 100000;
  public static final int DEFAULT_NGROUPS = 5;

  @Override
  public CGroupTranslator convert(String argument) {
    String translatorName = argument.trim().toUpperCase();
    if (CpuQuotaCGroupTranslator.NAME.equals(translatorName)) {
      return new CpuQuotaCGroupTranslator(DEFAULT_NGROUPS, DEFAULT_CPU_PERIOD);
    }
    if (CpuSharesCGroupTranslator.NAME.equals(translatorName)) {
      return new CpuSharesCGroupTranslator();
    }

    throw new IllegalArgumentException(
        String.format("Unknown cgroup translator requested: %s", argument));
  }

}
