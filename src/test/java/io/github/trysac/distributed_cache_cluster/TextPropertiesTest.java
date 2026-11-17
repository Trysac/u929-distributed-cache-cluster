package io.github.trysac.distributed_cache_cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.StringLength;

/** Property-based tests for Text. */
class TextPropertiesTest {

  @Property
  void normalizeIsIdempotent(@ForAll @AlphaChars @StringLength(min = 1, max = 50) String input) {
    String once = Text.normalize(input);
    assertEquals(once, Text.normalize(once));
  }
}
