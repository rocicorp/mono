import type { TouchEvent } from "react";

export function touchToMouse(
  e: TouchEvent,
  handler: ({ pageX, pageY }: { pageX: number; pageY: number }) => void
) {
  if (e.touches.length == 1) {
    handler(e.touches[0]);
  }
}
