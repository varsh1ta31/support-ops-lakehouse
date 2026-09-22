# Security Policy

## Reporting

Do not open a public issue containing a credential, customer record, vulnerability detail, or
other sensitive material. Report it privately to the repository owner through GitHub's private
vulnerability reporting feature when enabled.

## Repository rules

- Never commit Databricks tokens, cloud credentials, LLM API keys, or connection strings.
- Use environment variables, Databricks secrets, or workload identities.
- Use synthetic data only; do not add real customer or support-ticket content.
- Treat generated AI context and output as potentially sensitive operational data.
- Revoke and rotate any credential immediately if it is exposed.

The `.gitignore` excludes common local secret files and generated artifacts, but it is not a
substitute for reviewing changes before every commit.
