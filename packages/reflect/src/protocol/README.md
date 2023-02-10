ts-to-zod exists, and I would very much like to use it so that ts types were
canonical, not schemas.

However, ts-to-zod doesn't support recursive types like JSON even though zod
itself does! Gar. Oh well, we'll make the schemas canonical instead.
