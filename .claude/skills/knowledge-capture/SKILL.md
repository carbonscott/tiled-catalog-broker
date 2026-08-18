---
name: knowledge-capture
description: Work through a topic-organized knowledge base in knowledge/, one topic at a time — gathering explanations and capturing artifacts (videos, screenshots, diagrams). Creates the structure if it does not exist yet. Use when the user invokes /knowledge-capture, wants to capture notes on a topic, asks what is left to cover, or wants to pick up where they left off.
---

# Knowledge capture

A topic-organized knowledge base under `knowledge/`. Topics are built up one
at a time. Any presentation or deck is derived from the collected material
**at the end** — never the other way around.

## First: does the structure exist?

Check for `knowledge/INDEX.md`.

### If it does not exist — build it with the user

Do **not** invent topics. Invoke the `grill-me` skill to interview them
until you understand:

- What topics they want to capture, in their own terms
- What questions they want answered under each
- What artifacts they need — videos, screenshots, diagrams, terminal
  captures — and which topic each belongs to
- Anything already known that should be recorded now

Then create:

- `knowledge/INDEX.md` — a table of every topic file and one line on what
  is in it, plus the conventions below and any cross-cutting tasks
- One file per topic, using the anatomy below
- `knowledge/artifacts/` for captured media

Add `knowledge/` to `.gitignore` unless the user wants it committed.

Populate the checklists **only** with what the user actually asked for.
An empty section marked `_None identified yet._` is correct and honest;
a section padded with plausible-sounding items is not.

### If it exists — orient, then ask

Read `knowledge/INDEX.md` and every topic file. Then ask:

> What question/topic would you like to talk through? Here are some
> suggestions...

List what is still open, drawn from the actual unchecked items. Group by
topic file and lead with the ones that are unblocked. Mark blocked items
with who or what they are waiting on, so the user can see at a glance what
they could do right now versus what needs someone else.

Keep the list short enough to scan. If a topic is entirely untouched, say
so as one line rather than enumerating all of its items.

## When the user raises something not already in a topic file

**Clarify where it should be added — do not silently file it.**

- If it clearly belongs to an existing topic, say which and confirm:
  *"That sounds like it belongs under `benchmarks.md` — agreed?"*
- If it does not fit any existing topic, say so and propose a new one,
  with a name and a one-line description of what it would hold. Let the
  user accept, rename, or redirect it.
- If it could sit in two places, say both and ask which. Do not duplicate
  it across files — one home, referenced from elsewhere if needed.

## Working a topic

One topic at a time. Do not range ahead into other files.

1. Read the topic file's checklists.
2. Talk the question through with the user before writing anything. Their
   thinking is the content; your job is to draw it out, challenge it where
   it is vague, and then record it.
3. Research what you can verify yourself — code, commands, docs, the web —
   and bring evidence rather than assertions.
4. Write it into the `## Notes` section, then tick the checklist item.
5. For artifacts you cannot produce (video, screenshots), write the shot
   list or capture instructions into the file and leave the item open until
   the user confirms it is captured.

Never mark an item `[x]` on the strength of a plan. Done means the answer
is written down or the artifact is in `knowledge/artifacts/`.

## Topic file anatomy

```markdown
# <Topic>

<one or two lines on what this topic is and why it matters>

## Questions
- [ ] <question to answer>
- [!] <question> — blocked on <who/what>

## Artifacts
- [ ] <video / screenshot / diagram to capture>

## Notes
<content accumulates here, with sources>

## Open questions
```

Some topics also carry a `## Tasks` section for concrete actions (delete a
dataset, re-register it) that are not questions or artifacts.

## Conventions

- `[ ]` open · `[~]` partial · `[x]` done · `[!]` blocked (say on whom/what)
- **Record where a fact came from** — a command and its output, a file and
  line, a person and when. Notes destined for a talk must survive someone
  asking "how do you know?"
- **Keep raw captures separate from conclusions.** Paste the actual output;
  write the takeaway underneath. The takeaway changes; the output doesn't.
- **Say what wasn't verified.** A claim marked unverified is useful; one
  that silently isn't is a liability on stage.
- **Absolute dates**, never "last week".
- Artifacts live in `knowledge/artifacts/`, referenced from the topic file
  that needs them.

## Building the deck

Only once the topics are filled in and their tasks are complete. At that
point read `knowledge/talk-brief.md` for audience, length, and desired
outcomes, and build the deck from what the collection actually supports.
Do not shape the notes to fit a deck outline, and do not start the outline
early — deciding the shape of the talk before knowing what you have is the
failure mode this whole structure exists to avoid.
