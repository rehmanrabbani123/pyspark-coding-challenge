# Training Data Pipeline (PySpark)

This repository contains a PySpark pipeline for generating training input datasets designed for a transformer model. The objective is to prepare **action histories** for customers that align with impression events, facilitating the training of a new algorithm for the "Our top choices carousel".

---

## Training Input Structure

Each training record includes:
- `dt` – the date of the impression
- `customer_id` – a unique identifier for the customer
- `item_id` – the item shown in the impression
- `is_order` – indicates whether the impression led to an order
- `actions` – the customer’s last 1000 actions prior to the impression (padded/truncated as required)
- `action_types` – the types of those actions (1=click, 2=add-to-cart, 3=order)

This structure ensures every impression is paired with the correct **sequence of past behavior**.

---

## Pipeline Overview

The pipeline comprises modular PySpark functions:

1. **Build Actions**:
   - Consolidate clicks, add-to-carts, and orders into a single DataFrame with a standardized schema.

2. **Explode Impressions**:
   - Flatten impressions to represent one row per (customer, item).

3. **Customer Action History**:
   - Join impressions with actions.
   - Collect the last 1000 actions (within 1 year before the impression).
   - Maintain action ordering and align with action types.
   - Pad/truncate arrays to maintain consistent length.

4. **Final Training Dataset**:
   - Repartition by `dt` to enable efficient downstream reads and scalability.

---

## Performance Considerations

Our pipeline leverages several key strategies to efficiently manage large datasets:

- **Broadcast Joins**: Impressions are smaller compared to actions, so we use broadcast joins. This reduces shuffle operations and speeds up data processing by distributing smaller tables across all nodes.

- **Action Filtering**: We filter actions to those within a year before the latest date in our window, focusing only on customers seen in impressions. This concentrates analysis on relevant data and reduces computational load.

- **Window Functions**: Used selectively to rank actions by recency, ensuring we pull the most relevant 1000 prior actions without processing excess data. This maintains the efficiency of ordered data retrieval.

- **Repartition by Date (`dt`)**: Repartitioning by date optimizes data distribution for faster reads and writes, balancing workload across nodes and enhancing access performance for analytics.

- **Array Padding/Truncation**: To provide consistent sequence lengths for model training, we pad or truncate action sequences. This ensures our model receives fixed-length data efficiently.

- **Optimized Spark Configurations**: Our pipeline setups utilize dynamic resource allocation, shuffle partition tuning, and adaptive query execution to automatically adjust resources and execution plans based on real-time data feedback, enhancing efficiency across varying workloads.

These measures make the pipeline robust and scalable, adeptly handling large datasets with accuracy and efficiency.

---

## How to Use the Pipelines

- Clone the repository and establish a functional Spark environment, whether locally or via Databricks with suitable workspace configurations.
- Execute the notebook `pyspark-coding-challenge`, providing the generated DataFrames as inputs to produce training-ready datasets.
- Use `data_generation` to create test datasets; import its functions into your own notebook/package as necessary.

---

## Testing and Validation

- We leverage `pytest` to validate our PySpark transformations, ensuring output consistency and compliance with the specifications provided.

---

## Why this Pipelines?

The data pipeline is structured to:
- Accurately capture sequences of historical actions per user, maintaining fidelity imperative for effective machine learning model training.
- Optimize resource utilization while managing high data throughput at scale, promoting rigorous data movement and processing.
- Offer flexibility, allowing efficient ingestion of raw input data, equipping teams for diverse deployment scenarios.

---