# test-repo

[Assignment details in Confluence](https://markf.atlassian.net/wiki/x/AQAhBw)

This repository is used to practice the Hawk-Center Pull Request workflow.

## Workflow
1. Clone and enter the repository:
   ```bash
   git clone https://github.com/Hawk-Center/test-repo.git
   cd test-repo
   ```
2. Sync `main`:
   ```bash
   git checkout main
   git pull origin main
   ```
3. Create a feature branch:
   ```bash
   git checkout -b members/<firstname>-<lastname>-hello-file
   ```
4. Create exactly one file at the repository root:
   `hello_<firstname>_<lastname>.py`
5. Run the file locally:
   ```bash
   python hello_<firstname>_<lastname>.py
   ```
6. Commit and push your feature branch, then open a pull request to `main` for Maintainer review.

## Required Output
When run, your file must print:
`Hello World from  <firstname> <lastname>!`

Note: there are two spaces between `from` and `<firstname>`.
