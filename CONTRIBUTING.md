# Contributing

The public compatibility surface is `book`, `feed`, `feed/gencsv`, and
`replay`. Changes to exported identifiers in those packages should include Go
doc comments, tests, examples when useful, and a `CHANGELOG.md` entry.

Run the local checks before opening a pull request:

```sh
./scripts/install-tools.sh
./scripts/check.sh
```

On Windows:

```powershell
pwsh ./scripts/install-tools.ps1
pwsh ./scripts/check.ps1
```

Use `scripts/install-hooks.*` to install repository-managed Git hooks.

Before a release, follow `docs/RELEASE.md`.
