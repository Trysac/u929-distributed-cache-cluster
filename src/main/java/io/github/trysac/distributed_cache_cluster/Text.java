package io.github.trysac.distributed_cache_cluster;

import java.util.Locale;
import java.util.Objects;

/** Text helpers of this library. */
public final class Text {

  private Text() {}

  /**
   * Trims surrounding whitespace and lower-cases the text.
   *
   * @param input the text to normalize
   * @return the trimmed, lower-cased text
   * @throws BlankInputException when nothing is left after trimming, instead of an empty string
   * @throws NullPointerException when {@code input} is null
   */
  public static String normalize(String input) {
    Objects.requireNonNull(input, "input");
    String trimmed = input.strip();
    if (trimmed.isEmpty()) {
      throw new BlankInputException();
    }
    return trimmed.toLowerCase(Locale.ROOT);
  }
}
