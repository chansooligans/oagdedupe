# Classifier

Entity resolution is a binary classification problem, where each unit is a comparison pair, X is a matrix of distances between attributes, and the output is either a match or a non-match.

We use modAl, sklearn random forest classifier, and fastAPI. 

The first iteration trains the model using the fake data constructed from the sample at initialization. Uncertainty sampling is applied and samples are sent to label-studio for manual label.
