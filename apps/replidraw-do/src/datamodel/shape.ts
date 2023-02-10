import type { ReadTransaction, WriteTransaction } from "@rocicorp/reflect";
import { nanoid } from "nanoid";
import { z } from "zod";
import { randInt } from "../util/rand";

export const shapeSchema = z.object({
  type: z.literal("rect"),
  x: z.number(),
  y: z.number(),
  width: z.number(),
  height: z.number(),
  rotate: z.number(),
  fill: z.string(),
});

export type Shape = z.infer<typeof shapeSchema>;

export async function getShape(
  tx: ReadTransaction,
  id: string
): Promise<Shape | null> {
  const jv = await tx.get(key(id));
  if (!jv) {
    console.log(`Specified shape ${id} not found.`);
    return null;
  }
  return jv as Shape;
  //return shapeSchema.parse(jv);
}

export function putShape(
  tx: WriteTransaction,
  { id, shape }: { id: string; shape: Shape }
): Promise<void> {
  return tx.put(key(id), shape);
}

export async function deleteShape(
  tx: WriteTransaction,
  id: string
): Promise<void> {
  await tx.del(key(id));
}

export async function moveShape(
  tx: WriteTransaction,
  { id, dx, dy }: { id: string; dx: number; dy: number }
): Promise<void> {
  const shape = await getShape(tx, id);
  if (shape) {
    await putShape(tx, {
      id,
      shape: {
        ...shape,
        x: shape.x + dx,
        y: shape.y + dy,
      },
    });
  }
}

export async function scanShape(
  tx: WriteTransaction,
  { id, dx, maxX }: { id: string; dx: number; maxX: number }
): Promise<void> {
  const shape = await getShape(tx, id);
  if (!shape) {
    return;
  }
  let newX = (shape.x += dx);
  if (newX > maxX) {
    newX = 0;
  }
  putShape(tx, {
    id,
    shape: {
      ...shape,
      x: newX,
    },
  });
}

export async function resizeShape(
  tx: WriteTransaction,
  { id, ds }: { id: string; ds: number }
): Promise<void> {
  const shape = await getShape(tx, id);
  if (shape) {
    const minSize = 10;
    const dw = Math.max(minSize - shape.width, ds);
    const dh = Math.max(minSize - shape.height, ds);
    await putShape(tx, {
      id,
      shape: {
        ...shape,
        width: shape.width + dw,
        height: shape.height + dh,
        x: shape.x - dw / 2,
        y: shape.y - dh / 2,
      },
    });
  }
}

export async function rotateShape(
  tx: WriteTransaction,
  { id, ddeg }: { id: string; ddeg: number }
): Promise<void> {
  const shape = await getShape(tx, id);
  if (shape) {
    await putShape(tx, {
      id,
      shape: {
        ...shape,
        rotate: shape.rotate + ddeg,
      },
    });
  }
}

export async function initShapes(tx: WriteTransaction) {
  if (await tx.has("initialized")) {
    return;
  }
  const shapes = Array.from({ length: 5 }, () => randomShape());
  await Promise.all([
    tx.put("initialized", true),
    ...shapes.map((s) => putShape(tx, s)),
  ]);
}

function key(id: string): string {
  return `${shapePrefix}${id}`;
}

export const shapePrefix = "shape-";

const colors = ["red", "blue", "white", "green", "yellow"];
let nextColor = 0;

export function randomShape() {
  const s = randInt(100, 400);
  const fill = colors[nextColor++];
  if (nextColor == colors.length) {
    nextColor = 0;
  }
  return {
    id: nanoid(),
    shape: {
      type: "rect",
      x: randInt(0, 400),
      y: randInt(0, 400),
      width: s,
      height: s,
      rotate: randInt(0, 359),
      fill,
    } as Shape,
  };
}
