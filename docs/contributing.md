# Contribute

Thank you for your interest in contributing to kedro-databricks! This document provides guidelines and instructions for contributing to this project.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Environment Setup](#development-environment-setup)
- [Development Workflow](#development-workflow)
- [Code Standards](#code-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Submitting Changes](#submitting-changes)
- [Release Process](#release-process)

## Getting Started

kedro-databricks is a Kedro plugin that enables running Kedro pipelines on Databricks. Before contributing, please:

1. Read the [README.md](README.md) to understand the project's purpose and features
2. Check the [documentation](https://kedro-databricks.readthedocs.io/) for detailed usage instructions
3. Look at existing [issues](https://github.com/JenspederM/kedro-databricks/issues) and [pull requests](https://github.com/JenspederM/kedro-databricks/pulls)

### Prerequisites

- Python 3.10, 3.11, or 3.12
- [uv](https://docs.astral.sh/uv/) (recommended) or pip for dependency management
- Git for version control
- A Databricks workspace (for integration testing)

## Development Environment Setup

### Using uv (Recommended)

1. **Clone the repository:**
   ```bash
   git clone https://github.com/JenspederM/kedro-databricks.git
   cd kedro-databricks
   ```

2. **Install dependencies:**
   ```bash
   uv sync --all-extras
   ```

3. **Activate the virtual environment:**
   ```bash
   source .venv/bin/activate  # On Unix/macOS
   # or
   .venv\Scripts\activate     # On Windows
   ```

4. **Install pre-commit hooks:**
   ```bash
   pre-commit install
   ```

### Using pip

1. **Clone and create virtual environment:**
   ```bash
   git clone https://github.com/JenspederM/kedro-databricks.git
   cd kedro-databricks
   python -m venv .venv
   source .venv/bin/activate  # On Unix/macOS
   ```

2. **Install in development mode:**
   ```bash
   pip install -e ".[dev,test,docs]"
   ```

3. **Install pre-commit hooks:**
   ```bash
   pre-commit install
   ```

### Development Scripts

The project includes several helpful development scripts in the `scripts/` directory:

- `scripts/mkdev.sh <project_name>`: Create a new Kedro project for development/testing
- `scripts/run_lint.sh`: Run linting checks
- `scripts/validate_codecov.sh`: Validate code coverage configuration

## Development Workflow

### Branch Naming Convention

Use descriptive branch names with prefixes:

- `feat/`: New features
- `fix/`: Bug fixes
- `docs/`: Documentation changes
- `test/`: Test-related changes
- `chore/`: Maintenance tasks
- `refactor/`: Code refactoring
- `ci/`: CI/CD changes

Examples:
- `feat/add-multi-cloud-support`
- `fix/databricks-authentication`
- `docs/update-getting-started`

### Commit Message Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/) enforced by Commitizen:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types:**
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that do not affect the meaning of the code
- `refactor`: A code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests or correcting existing tests
- `chore`: Changes to the build process or auxiliary tools

**Examples:**
```
feat: add support for multi-cloud deployments
fix: resolve authentication issue with Databricks SDK
docs: update installation instructions
test: add integration tests for deployment workflow
```

## Code Standards

### Linting and Formatting

The project uses [Ruff](https://docs.astral.sh/ruff/) for both linting and formatting:

```bash
# Run linting
ruff check .

# Run formatting
ruff format .

# Fix auto-fixable issues
ruff check --fix .
```

### Code Quality Rules

- Line length: 88 characters (managed by Ruff)
- Follow PEP 8 style guidelines
- Use type hints where possible
- Write descriptive variable and function names
- Add docstrings for public functions and classes

### Pre-commit Hooks

Pre-commit hooks automatically run quality checks before each commit:

- **Commitizen**: Validates commit message format
- **Ruff**: Code linting and formatting
- **YAML/TOML validation**: Ensures configuration files are valid
- **Trailing whitespace**: Removes trailing spaces
- **End of file fixer**: Ensures files end with newlines
- **Codecov validation**: Custom validation for coverage configuration

## Testing

### Test Structure

Tests are organized into two categories:

- **Unit tests**: `tests/unit/` - Fast, isolated tests
- **Integration tests**: `tests/integration/` - Tests requiring external services

### Running Tests

```bash
# Run all tests
pytest

# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/

# Run with coverage
pytest --cov=kedro_databricks --cov-report=html
```

> NOTE: Ensure you have a valid Databricks configuration for integration tests.

### Test Requirements

- **Coverage**: Minimum 70% code coverage required
- **Test isolation**: Each test should be independent
- **Fixtures**: Use pytest fixtures for common test setup (see `tests/conftest.py`)
- **Mocking**: Mock external dependencies in unit tests

### Writing Tests

1. **Test file naming**: `test_<module_name>.py`
2. **Test function naming**: `test_<function_name>_<scenario>`
3. **Use descriptive assertions**: Prefer specific assertions over generic ones
4. **Test both success and failure cases**

Example:
```python
def test_deploy_command_with_valid_config(cli_runner, tmp_path):
    """Test that deploy command succeeds with valid configuration."""
    # Arrange
    config_file = tmp_path / "databricks.yml"
    config_file.write_text("valid: config")

    # Act
    result = cli_runner.invoke(deploy_command, ["--config", str(config_file)])

    # Assert
    assert result.exit_code == 0
    assert "Deployment successful" in result.output
```

## Documentation

### Building Documentation

The project uses [MkDocs](https://www.mkdocs.org/) with Material theme:

```bash
# Install documentation dependencies
uv sync --extra docs

# Serve documentation locally
mkdocs serve

# Build documentation
mkdocs build
```

### Documentation Standards

- **API Documentation**: Use docstrings with [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
- **Examples**: Add examples to the `examples/` directory with README files. Any example should be runnable and demonstrate key features.
- **Changelog**: Automatically generated from conventional commits

### Adding Examples

1. Create a new directory in `examples/` with a descriptive name
2. Include a `README.md` explaining the example
3. Add all necessary configuration files (`databricks.yml`, `resources.yml`, etc.)
4. Update the documentation index if needed

## Submitting Changes

### Pull Request Process

1. **Fork the repository** and create a feature branch
2. **Make your changes** following the guidelines above
3. **Write or update tests** for your changes
4. **Update documentation** if needed
5. **Ensure all checks pass**:
   ```bash
   # Run pre-commit checks
   pre-commit run --all-files

   # Run tests
   pytest

   # Check coverage
   pytest --cov=kedro_databricks --cov-fail-under=70
   ```
6. **Submit a pull request** with:
   - Clear title and description
   - Reference to related issues
   - Screenshots/examples if applicable

### Pull Request Template

When creating a pull request, include:

- **What**: Brief description of changes
- **Why**: Motivation for the changes
- **How**: Technical approach taken
- **Testing**: How changes were tested
- **Documentation**: Any documentation updates

### Review Process

- All pull requests require review from maintainers
- CI checks must pass (linting, tests, coverage)
- Address feedback promptly and professionally
- Maintainers will merge once approved

## Release Process

Releases are automated using Commitizen and follow semantic versioning:

1. **Version bumping**: Automated based on conventional commits
2. **Changelog generation**: Automatically created from commit messages
3. **GitHub releases**: Created automatically on version tags
4. **PyPI publishing**: Automated through CI/CD pipeline

### Manual Release (Maintainers)

```bash
# Bump version and create changelog
cz bump

# Push tags
git push --tags
```

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/JenspederM/kedro-databricks/issues)
- **Pull Requests**: [GitHub Pull Requests](https://github.com/JenspederM/kedro-databricks/pulls)
- **Documentation**: [Read the Docs](https://kedro-databricks.readthedocs.io/)

## Code of Conduct

Please be respectful and professional in all interactions. We welcome contributions from everyone and strive to create an inclusive environment.

---

Thank you for contributing to kedro-databricks! 🚀
