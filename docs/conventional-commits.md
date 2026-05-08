# Conventional Commits

This project uses [Conventional Commits](https://www.conventionalcommits.org/) to ensure clean, readable version history and to enable automated changelog generation.

## Pull Request Requirements

All PR titles must follow the Conventional Commit format:

```
<type>: <description>
```

For example:

```
feat: add support for STAC item collections
fix: resolve NaN handling in time series output
docs: update API examples with new backend options
```

## Supported Types

The following types are accepted:

- `feat` — new features or enhancements
- `fix` — bug fixes
- `docs` — documentation changes
- `test` — additions or corrections to tests
- `ci` — changes to CI/CD configuration
- `refactor` — code changes that neither fix bugs nor add features
- `perf` — performance improvements
- `chore` — routine maintenance tasks (dependency updates, formatting, etc.)
- `revert` — reverting a previous change

## Why This Matters

Using conventional commits allows us to:

- Generate changelogs automatically
- Determine semantic version bumps (MAJOR, MINOR, PATCH) automatically
- Keep the project history clean and scannable

## Resources

- [Conventional Commits Specification](https://www.conventionalcommits.org/)
