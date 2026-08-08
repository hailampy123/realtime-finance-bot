resource "random_password" "sasl" {
  length  = 32
  special = false
}

resource "aws_kms_key" "secrets" {
  description             = "${var.project} MSK SCRAM secret encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

resource "aws_kms_alias" "secrets" {
  name          = "alias/${var.project}-msk-scram"
  target_key_id = aws_kms_key.secrets.key_id
}

# MSK requires the secret name to start with "AmazonMSK_".
resource "aws_secretsmanager_secret" "scram" {
  name                    = "AmazonMSK_${var.project}_producer"
  kms_key_id              = aws_kms_key.secrets.arn
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "scram" {
  secret_id = aws_secretsmanager_secret.scram.id
  secret_string = jsonencode({
    username = "${var.project}-producer"
    password = random_password.sasl.result
  })
}

# A resource policy, not an IAM role — grants the MSK service read access.
resource "aws_secretsmanager_secret_policy" "scram" {
  secret_arn = aws_secretsmanager_secret.scram.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AWSKafkaResourcePolicy"
      Effect    = "Allow"
      Principal = { Service = "kafka.amazonaws.com" }
      Action    = "secretsmanager:getSecretValue"
      Resource  = aws_secretsmanager_secret.scram.arn
    }]
  })
}

# MSK refuses UpdateConnectivity unless allow.everyone.if.no.acl.found is
# false, and that property only reaches the brokers through a configuration --
# there is no cluster-level argument for it.
#
# The value is a variable rather than a constant because the ordering is
# load-bearing. false turns Kafka's authorizer to deny-by-default, which locks
# out every SCRAM principal, including the one that would create the ACLs. The
# cluster is therefore born permissive, gets its ACLs (scripts/create_acls.py,
# run from inside the VPC), and only then is tightened. AWS documents the same
# order: "you must first set Apache Kafka ACLs for your cluster. Then, update
# the cluster's configuration".
#
# server_properties is not ForceNew, so flipping this edits the configuration
# in place and publishes a new revision -- the cluster is never replaced, and
# the flip is reversible if a bootstrap goes wrong.

# A deleted MSK configuration lingers in DELETING for a while and its name
# stays taken, which would make `make rebuild` fail on the re-create. The
# suffix is stable in state, so flipping restrict_acls does not rename it
# (name is ForceNew and a rename would replace the cluster).
resource "random_id" "config" {
  byte_length = 4
}

resource "aws_msk_configuration" "this" {
  name        = "${var.project}-kafka-${random_id.config.hex}"
  description = "${var.project}: ACL enforcement toggle for MSK public access"

  server_properties = <<-PROPERTIES
    allow.everyone.if.no.acl.found=${var.restrict_acls ? "false" : "true"}
  PROPERTIES
}

resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.project}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [var.security_group_id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_ebs_gb
      }
    }

    dynamic "connectivity_info" {
      for_each = var.public_access ? [1] : []
      content {
        public_access {
          type = "SERVICE_PROVIDED_EIPS"
        }
      }
    }
  }

  client_authentication {
    sasl {
      scram = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }
}

resource "aws_msk_scram_secret_association" "this" {
  cluster_arn     = aws_msk_cluster.this.arn
  secret_arn_list = [aws_secretsmanager_secret.scram.arn]

  depends_on = [aws_secretsmanager_secret_version.scram]
}
