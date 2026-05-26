#!/usr/bin/env python3
"""Local data generation wrapper for Lab5 brand sentiment events."""

import argparse
import subprocess
import sys

from .common.cloud_detection import auto_detect_cloud_provider, suggest_cloud_provider
from .common.logging_utils import setup_logging
from .common.terraform import get_project_root, validate_terraform_state


DEFAULT_DATA_FILE = "assets/lab5/data/brand_mentions.jsonl"


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lab5_datagen",
        description="Publish local Lab5 brand sentiment data to Kafka with Schema Registry",
    )
    parser.add_argument(
        "cloud_provider",
        nargs="?",
        choices=["aws", "azure"],
        help="Target cloud provider (auto-detected if not specified)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate setup without publishing messages",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed output",
    )
    return parser


def main() -> None:
    args = create_argument_parser().parse_args()
    logger = setup_logging(args.verbose)
    project_root = get_project_root()

    cloud_provider = args.cloud_provider or auto_detect_cloud_provider()
    if not cloud_provider:
        logger.error("Could not auto-detect cloud provider")
        suggest_cloud_provider(project_root)
        sys.exit(1)

    if not validate_terraform_state(cloud_provider, project_root):
        logger.error("Terraform state validation failed")
        logger.error(
            "Please deploy terraform/core/ and terraform/lab5-brand-sentiment-response/ first"
        )
        sys.exit(1)

    cmd = ["uv", "run", "publish_lab5_data", "--data-file", DEFAULT_DATA_FILE]
    if args.dry_run:
        cmd.append("--dry-run")
    if args.verbose:
        cmd.append("--verbose")

    result = subprocess.run(cmd, cwd=project_root)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
