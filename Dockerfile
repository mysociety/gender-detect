FROM python:3.14-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.12.9 /uv /uvx /bin/

ENV UV_LINK_MODE=copy
ENV UV_PROJECT_ENVIRONMENT=/workspaces/gender-detect/.venv
ENV PATH="/workspaces/gender-detect/.venv/bin:${PATH}"
