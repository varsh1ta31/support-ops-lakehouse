# Contributing

## Workflow

1. Create a short-lived branch from `main`.
2. Keep changes focused on one component or concern.
3. Add or update tests with implementation changes.
4. Run the repository checks documented in the README before opening a pull request.
5. Open a pull request and complete its verification checklist.

Use Conventional Commit-style subjects where practical, for example:

```text
feat(ingestion): add idempotent account loading
fix(quality): quarantine invalid priority values
docs(architecture): record watermark decision
```

## Engineering expectations

- Put reusable business logic in `src/`, not notebooks.
- Keep environment-specific values in configuration.
- Make data writes idempotent and safe to rerun.
- Preserve malformed source records for diagnosis.
- Use UTC for stored timestamps.
- Do not commit credentials, generated datasets, checkpoints, or model artifacts.
- Include measured evidence for performance claims.

## Pull-request scope

Pull requests should state what changed, how it was verified, cost impact, and any data or
deployment implications. Architecture changes should include an architecture decision record.
