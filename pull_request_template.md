# <Title_Here>
## What?
-
## Why?
-
## CR strategy
Goal: guide the reviewer through the code
### CR checklist
- [ ] Scope of CR (does PR include "demo" code)?
- [ ] Does CR require a technical understanding of another concept/repo? If so, what links?
- [ ] Prioritization - are there files/functions that should be reviewed before others? Maybe due to algorithmic complexity, dependencies within the PR, etc.
## PR Checklist
- [ ] Merged latest main
- [ ] Updated version number
- [ ] Version numbers match between package `_version.py` and `pyproject.toml`
- [ ] Ran `poetry update` and committed `pyproject.toml` and `poetry.lock`
## Breaking Changes