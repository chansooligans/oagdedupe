# Active Learning Loop

As uncertainty samples are labeled, label-studio sends labels to fastAPI. When there are fewer than 5 tasks left on label-studio, fastAPI generates new samples.

To generate new samples, we first append the newly labeled pairs to schema.labels. Next we delete all records in schema.train that have not been labeled. A new sample of size settings.other.n is pulled and appended to schema.train.

Next, we re-evaluate blocks to obtain best block conjunctions. The metrics are now computed on this new sample and the updated labels are used to evaluate positive and negative coverage.

Again, we apply the block conjunctions to get comparison pairs then re-train the active learner with newly labeled data. Finally, the active learner submits new uncertain samples to label-studio.