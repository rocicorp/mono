/**
 * Configures the view of the upstream database for a zero-cache shard.
 */
export type ShardConfig = {
  /**
   * Unique identifier for the zero-cache shard. This is used to partition
   * sharded tables such as `zero.clients`, as well as reserve a name for
   * the replication slot.
   */
  readonly id: string;

  /**
   * List of Postgres `PUBLICATION`s that the shard subscribes to.
   * Publications for application data begin with the `"zero_"` prefix,
   * and publications for zero metadata (e.g. client lmids, schema versions),
   * begin with `"_zero_"`.
   */
  readonly publications: readonly string[];
};
