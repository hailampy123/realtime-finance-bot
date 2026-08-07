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

resource "aws_msk_cluster" "this" {
  cluster_name           = "${var.project}-kafka"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

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
