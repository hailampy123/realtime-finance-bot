data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }
}

resource "aws_instance" "producer" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = [var.security_group_id]
  associate_public_ip_address = true

  # Null unless the account provides a pre-existing profile; we never create one.
  iam_instance_profile = var.instance_profile_name

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    repo_url          = var.repo_url
    repo_ref          = var.repo_ref
    bootstrap_servers = var.bootstrap_servers
    sasl_username     = var.sasl_username
    sasl_password     = var.sasl_password
    venues_json       = jsonencode(var.venues)
  })

  user_data_replace_on_change = true

  root_block_device {
    volume_size = 20
    encrypted   = true
  }

  tags = { Name = "${var.project}-producer" }
}
