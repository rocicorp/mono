import * as v from '../../shared/src/valita.ts';

/**
 * All ZeroEvents are routed via CloudEvents frameworks. The `type`
 * and `time` fields of ZeroEvents are directly used for the corresponding
 * fields in the CloudEvent, while the entire ZeroEvent object is set as the
 * `data` field.
 *
 * The CloudEvent `type` field can thus be used to filter events and/or parse
 * their corresponding subtypes.
 */
export const zeroEventSchema = v.object({
  /**
   * Identifies the event type across all components.
   *
   * When published as a CloudEvent, this values is also used for the "type"
   * field of the outer event object.
   */
  type: v.string(),

  /**
   * The time that the status event was emitted, in ISO 8601 format. Later
   * statuses override earlier ones.
   *
   * When published as a CloudEvent, this value is also used for the "time"
   * field of the outer event object.
   */
  time: v.string(),
});

export type ZeroEvent = v.Infer<typeof zeroEventSchema>;
