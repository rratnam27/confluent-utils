#!/usr/bin/env bash
TAG=main-0007-893c60b
gh api /repos/urbn/x35-setup/contents/init.sh --jq '.content' | base64 -d | bash -s -- ${TAG}

source venv/bin/activate
pre-commit install
pre-commit run --all-files
