package io.palyvos.scheduler;

import java.util.List;
import org.testng.Assert;

public class AssertHelper {

  public static void assertNoNullElements(List<?> actual) {
    for (int i = 0; i < actual.size(); i++) {
      Assert.assertNotNull(actual.get(0), String.valueOf(i));
    }
  }

  private AssertHelper() {

  }

}
