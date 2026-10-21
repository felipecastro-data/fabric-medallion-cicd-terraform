## What & why
<!-- What does this change and why is it needed? -->

## Type
- [ ] Infrastructure (Terraform)
- [ ] Fabric items (notebooks, pipelines, environments)
- [ ] Python package / tests
- [ ] CI/CD
- [ ] Docs

## Checklist
- [ ] `ci` checks are green (lint, unit tests, Fabric item validation)
- [ ] Terraform plan reviewed (if `infra/` changed) — no unexpected destroys
- [ ] Fabric item names follow conventions (`lh_`, `nb_`, `pl_`, `env_`, `vl_`)
- [ ] Data-quality rules updated/added for new or changed tables
- [ ] Docs / README updated if behaviour changed
