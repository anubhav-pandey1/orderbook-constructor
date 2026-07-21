# Contributing

Run the local checks before opening a pull request:

```sh
./scripts/check.sh
```

On Windows:

```powershell
pwsh ./scripts/check.ps1
```

The public compatibility surface is `book`, `feed`, `feed/gencsv`, and
`replay`. Changes to exported identifiers in those packages should include docs,
tests, examples when useful, and a `CHANGELOG.md` entry.

Use `scripts/install-hooks.*` to install repository-managed Git hooks.
