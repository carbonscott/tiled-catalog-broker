---
hide:
  - navigation
  - toc
---

# Tiled Catalog Broker

Register scientific HDF5 datasets in a [Tiled](https://blueskyproject.io/tiled/) catalog, then find them by their physics and read them from anywhere.

We create a YAML that describes a dataset, and then we will use it to register the data with [Tiled](https://blueskyproject.io/tiled/). Your data keeps the names it already uses.

Describe a dataset once, and publish it:

```bash
tcb stamp-key datasets/mydata.yml   # name it
tcb generate  datasets/mydata.yml   # describe it
tcb register  datasets/mydata.yml   # publish it
```

From then on anyone who can reach the server can query it by its physics and read
an array without knowing where the files live:

```python
c = from_uri(URL, api_key=KEY)
hits = c["BROAD_SIGMA"].search(Key("sigma") >= 0.04)
hits.values().first()["rixs_spectrum"][:]     # (151, 40)
```

<div class="grid cards" markdown>

- :material-download:{ .lg .middle } **Start here**

    ---

    Clone the repo and install `tcb`. Reading a catalog someone else built needs only the Tiled client.

    [:octicons-arrow-right-24: Install](install.md)

- :material-school:{ .lg .middle } **MAIQMag all-hands**

    ---

    Once `tcb` is installed: get your key working and check your data is in a shape the catalog accepts. 15–20 minutes, before the session.

    [:octicons-arrow-right-24: Prepare for the workshop](workshop-prep.md)

- :material-lightbulb-on:{ .lg .middle } **Understand**

    ---

    The concepts behind this broker: what it adds to Tiled, and the data model it registers.

    [:octicons-arrow-right-24: Explanation](explanation/broker-and-tiled.md)

- :material-book-open-page-variant:{ .lg .middle } **Reference**

    ---

    Look up the `tcb` commands, every dataset YAML field, and the Parquet manifest columns.

    [:octicons-arrow-right-24: Reference](reference/cli.md)

</div>
