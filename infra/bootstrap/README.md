# Terraform state bootstrap

Creates the S3 bucket and DynamoDB lock table that `infra/envs/dev` uses as its
backend. Uses **local state**, because a backend cannot store its own state.

Losing this state is normally a disaster. Here it is not: the sandbox account is
wiped every 7 days, so the state and the resources it describes disappear
together and stay consistent (both empty). `make up` re-runs this layer from
scratch each cycle.

Run directly only for debugging — `make up` invokes it.
