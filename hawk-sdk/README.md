## Hawk SDK

https://hawk-center.github.io/hawk-sdk/

### Contributing

Please read [CONTRIBUTING.md](.github/CONTRIBUTING.md) for more details.

Here’s a sample README section for updating a pip package using `twine`:

---

## Updating the Package on PyPI

Follow these steps to update and publish your package on PyPI using `twine`:

### 1. Update `setup.py`

Ensure your `setup.py` file reflects the correct metadata and version for the new release. Update the version number in
accordance with [Semantic Versioning](https://semver.org/).

```python
version = 'X.Y.Z',  # Update this with the new version number
```

### 2. Install Required Tools

Make sure you have the necessary tools installed:

```bash
pip install setuptools wheel twine
```

### 3. Clean Up Previous Builds (Optional)

If you have previously built distributions in the `dist/` or `build/` directories, remove them to avoid conflicts:

```bash
rm -rf dist/ build/
```

### 4. Build the Package

Generate the source distribution (`sdist`) and the wheel (`bdist_wheel`) for your package:

```bash
python setup.py sdist bdist_wheel
```

This creates distribution files in the `dist/` directory.

### 5. Test the Package Locally (Optional)

You can test the package locally before uploading it to PyPI:

```bash
pip install dist/your_package_name-X.Y.Z-py3-none-any.whl
```

### 6. Upload to PyPI

Once you’re satisfied with the package, upload it to PyPI. Contact Rithwik for the pypi API key:

```bash
twine upload dist/*
```

### 8. Verify the Package

After uploading, verify the package has been successfully updated on PyPI by visiting your project page:

[https://pypi.org/project/your-package-name/](https://pypi.org/project/hawk-sd/)
