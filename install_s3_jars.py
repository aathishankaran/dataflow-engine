#!/usr/bin/env python3
"""
One-time setup: download hadoop-aws and aws-java-sdk-bundle JARs into
PySpark's jars/ directory so S3AFileSystem is available without any
runtime Maven/Ivy network resolution.

Usage (run once before starting the dataflow engine):
    python install_s3_jars.py

The script auto-detects your installed PySpark version and picks the
matching JAR versions:
    PySpark 3.0 – 3.1.x  →  hadoop-aws 3.2.0  /  aws-java-sdk-bundle 1.11.375
    PySpark 3.2+          →  hadoop-aws 3.3.1  /  aws-java-sdk-bundle 1.11.901

JARs are downloaded from Maven Central (https://repo1.maven.org).
If your environment uses a corporate Maven mirror, set:
    MAVEN_MIRROR=https://your-internal-mirror/repository/maven-central
before running this script.
"""

import os
import sys
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Version matrix  (pyspark_minor → (hadoop_aws_ver, aws_sdk_bundle_ver))
# ---------------------------------------------------------------------------
_VERSION_MAP = {
    (3, 0): ("3.2.0", "1.11.375"),
    (3, 1): ("3.2.0", "1.11.375"),
    (3, 2): ("3.3.1", "1.11.901"),
    (3, 3): ("3.3.4", "1.12.261"),
    (3, 4): ("3.3.4", "1.12.261"),
    (3, 5): ("3.3.4", "1.12.261"),
}
_DEFAULT_VERSIONS = ("3.3.4", "1.12.261")

_MAVEN_CENTRAL = os.environ.get(
    "MAVEN_MIRROR", "https://repo1.maven.org/maven2"
).rstrip("/")


def _jar_url(group_path: str, artifact: str, version: str) -> str:
    return "{base}/{g}/{a}/{v}/{a}-{v}.jar".format(
        base=_MAVEN_CENTRAL, g=group_path, a=artifact, v=version
    )


def _download(url: str, dest: Path) -> None:
    if dest.exists():
        LOG.info("Already present – skipping: %s", dest.name)
        return
    LOG.info("Downloading %s ...", url)
    try:
        import requests  # already in requirements.txt
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
        LOG.info("Saved  →  %s  (%d KB)", dest, len(resp.content) // 1024)
    except Exception as exc:
        LOG.error("Failed to download %s: %s", url, exc)
        raise


def main() -> None:
    try:
        import pyspark
    except ImportError:
        LOG.error("pyspark is not installed. Run: pip install -r requirements.txt")
        sys.exit(1)

    pv = tuple(int(x) for x in pyspark.__version__.split(".")[:2])
    hadoop_aws_ver, aws_sdk_ver = _VERSION_MAP.get(pv, _DEFAULT_VERSIONS)

    jars_dir = Path(pyspark.__file__).parent / "jars"
    LOG.info("PySpark version : %s", pyspark.__version__)
    LOG.info("Target jars dir : %s", jars_dir)
    LOG.info("hadoop-aws      : %s", hadoop_aws_ver)
    LOG.info("aws-java-sdk-bundle: %s", aws_sdk_ver)

    jars = [
        (
            _jar_url("org/apache/hadoop", "hadoop-aws", hadoop_aws_ver),
            jars_dir / "hadoop-aws-{}.jar".format(hadoop_aws_ver),
        ),
        (
            _jar_url("com/amazonaws", "aws-java-sdk-bundle", aws_sdk_ver),
            jars_dir / "aws-java-sdk-bundle-{}.jar".format(aws_sdk_ver),
        ),
    ]

    for url, dest in jars:
        _download(url, dest)

    LOG.info("S3A JARs ready. You can now run the dataflow engine.")


if __name__ == "__main__":
    main()
