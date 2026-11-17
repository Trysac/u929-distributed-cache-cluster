package io.github.trysac.distributed_cache_cluster;

import java.io.Serial;

/** Thrown when a required text input is empty or blank. */
public final class BlankInputException extends IllegalArgumentException {

  @Serial private static final long serialVersionUID = 1L;

  /** Creates a new exception indicating that the input is blank. */
  public BlankInputException() {
    super("input is blank");
  }
}
