package io.github.trysac.distributed_cache_cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

/** Property tests (jqwik) of the invariants of {@link Text#normalize}. */
class TextPropertiesTest {

  @Property
  void normalizingTwiceChangesNothing(@ForAll String input) {
    if (!input.isBlank()) {
      String once = Text.normalize(input);
      assertEquals(once, Text.normalize(once));
    }
  }

  @Property
  void outputHasNoSurroundingWhitespace(@ForAll String input) {
    if (!input.isBlank()) {
      String out = Text.normalize(input);
      assertEquals(out.strip(), out);
    }
  }
}
