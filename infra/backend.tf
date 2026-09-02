# Partial configuration. The bucket and key differ per deployment, so they are
# supplied at init rather than committed:
#
#   terraform init \
#     -backend-config="bucket=<state-bucket>" \
#     -backend-config="key=gz-csv-bulk-load/terraform.tfstate" \
#     -backend-config="region=<region>"
terraform {
  backend "s3" {}
}
