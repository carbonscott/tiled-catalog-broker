# How to prepare for the workshop

Do this before the session. It takes about 15–20 minutes. Bring questions if
anything does not work.

**Before you start:** [install `tcb`](install.md) and confirm that `tcb --help` prints
its four commands. Send the organizer any output that does not look right.

## 1. Optionally, install a coding agent

If you use [Claude Code](https://claude.ai/code) or something similar, this repo
ships an `/onboarding` skill that inspects your HDF5 files and drafts the dataset
configuration for you. It is the easier path and it is usually what gets
demonstrated. Everything is doable by hand if you prefer.

## 2. Get your data ready

This is the part most worth doing carefully in advance.

| | It has to be | If that is a problem |
| --- | --- | --- |
| **Where** | On the machine you installed on. The session uploads from your local files. | Data on a cluster: install and run there, or copy a subset down. |
| **Format** | HDF5, meaning anything `h5py` can open. The extension does not matter (`.h5`, `.hdf5`, `.nxs`, or none) as long as `file_pattern` matches it; the default is `**/*.h5`. NeXus files count, and ship a worked example (`datasets/examples/per_entity_nexus.yml`). | `.npy`, CSV, TIFF and proprietary instrument binaries will not work. If you are unsure, ask before the session rather than assuming. |
| **Structure** | Many comparable entities that share a structure, each with its own parameters and one or more arrays, in one of the [three layouts](explanation/layouts.md): one file per entity, entities stacked on axis 0 of one file, or one group per entity. | If yours matches none of them, say so beforehand. Mapping is usually possible, but better sorted in advance. |
| **Size** | About 1 GB. The upload moves the real bytes over the network, and everyone does it at once. | Bring a subset. `tcb register -n 5` registers only the first few entities, and it will be used. |
| **Provenance** | Know what produced the data (the instrument, or the code and its version) and the material or system studied. | Anything else you want recorded is welcome too. |

You do **not** need to rename parameters, adopt a shared schema, convert file
format, or match anyone else's columns.

## 3. Set up your access key

Your organizer will send you an API key separately — typically through a one-time
link that expires, so open it when it arrives rather than leaving it until the day.

--8<-- "connect.md"

If the check reports `401`, ask the organizer to reissue the key.

## 4. Do a practice upload

If you have never run `tcb register --upload` before, rehearse it now with
synthetic data instead of your own: [how to do a practice upload](practice-upload.md)
walks a marimo notebook through the same three commands against tiled-test, a
server kept for exactly this. It takes about five minutes and cannot collide with
anyone else's data.

## 5. Choose a name for your dataset

Everyone registers into the same shared catalog, and the container key is derived
from the name you choose. Prefix yours with your surname or initials — `Okafor NiPS3
Powder` rather than `My Dataset`. Two people picking the same name land in the same
container: not destructive, but confusing to untangle mid-session.

---

## Checklist

- [ ] `tcb --help` runs
- [ ] Data is HDF5, on this machine, under ~1 GB, in one of the three layouts
- [ ] `.env` written and loaded, and the check command lists dataset keys
- [ ] (optional) Practice-uploaded a fake dataset to tiled-test
- [ ] I have picked a dataset name with my surname in it

On the day, the session works through
[How to publish a dataset](ONBOARDING.md).
