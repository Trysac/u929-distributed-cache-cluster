package io.github.trysac.distributed_cache_cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests of the public API. */
class TextTest {

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {"'  Hello World '|hello world", "ABC|abc", "'\tx\n'|x"})
  void trimsAndLowercases(String input, String want) {
    assertEquals(want, Text.normalize(input));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   ", "\t\n"})
  void rejectsBlankInput(String input) {
    BlankInputException error =
        assertThrows(BlankInputException.class, () -> Text.normalize(input));
    assertEquals("input is blank", error.getMessage());
  }

  @Test
  void rejectsNull() {
    assertThrows(NullPointerException.class, () -> Text.normalize(null));
  }
}
