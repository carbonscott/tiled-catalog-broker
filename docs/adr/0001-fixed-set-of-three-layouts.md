# Fixed set of three supported layouts

The broker supports exactly three dataset **layouts** — `per_entity`, `batched`, and
`grouped` — and treats this set as frozen. New layouts require a new ADR.

We chose this over (a) collapsing to a single canonical layout and (b) accepting arbitrary
producer arrangements. A single layout would force every producer to physically reshape
existing data (including the 19.5 GB grouped Zhantao set from the #72 study); arbitrary
layouts are what make `tcb inspect`/`tcb generate` complex and is explicitly out of scope
("we do not support an arbitrary number of dataset configurations"). Three fixed layouts
cover the real producer data we have while keeping the data contract small enough to
document precisely and onboard against — by an agent or a human — without code reference.

Consequence: the cost of matching a layout moves to the producer (documented up front),
not to broker heuristics that guess at structure.
