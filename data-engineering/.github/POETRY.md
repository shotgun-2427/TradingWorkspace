# **Poetry Setup**

1. **Ensure you have Poetry installed**  
   If not, install it with:
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   ```

2. **Create a virtual environment and install dependencies**  
   ```bash
   poetry install
   ```

3. **Activate the virtual environment**  
   ```bash
   poetry shell
   ```

---

## **Usage**

To run scripts inside the Poetry environment:

```bash
poetry run python script.py
```

Or, if inside the Poetry shell:

```bash
python script.py
```

To add new dependencies:

```bash
poetry add <package-name>
```