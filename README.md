# Desafio Lighthouse - Dados & AI (2026)

## Demo

[Editable Notebook](https://vb-ferreira.github.io/desafio-lighthouse-2026/)

## Installation

These instructions assume you have `uv` installed. If not, you can install it using the [official documentation instructions](https://docs.astral.sh/uv/getting-started/installation/) (e.g., via `curl` on macOS/Linux or PowerShell on Windows).

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/vb-ferreira/desafio-lighthouse-2026.git
    cd desafio-lighthouse-2026
    ```

2.  **Set up the project environment:**
    `uv` will automatically create a virtual environment (`.venv` folder by default) and install all dependencies listed in `pyproject.toml` when you run the `sync` command.
    ```bash
    uv sync
    ```

## Usage

You can **run the notebook** using `uv run` to ensure it executes within the isolated project environment where dependencies are installed.

```bash
uv run python desafio.py
```

> When prompted to _Run in a sandbox venv containing this notebook's dependencies [Y/n]?_ **Select "no"**.
