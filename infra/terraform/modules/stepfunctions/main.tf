variable "project_name" { type = string }
variable "sfn_role_arn" { type = string }

resource "aws_sfn_state_machine" "etl" {
  name     = "${var.project_name}-etl-state-machine"
  role_arn = var.sfn_role_arn

  definition = file("${path.root}/../../../stepfunctions/global_partner_etl_state_machine.json")
}
