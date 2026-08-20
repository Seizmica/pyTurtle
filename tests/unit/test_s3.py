"""Unit tests for the S3 output utility (pure logic; no SparkSession needed)."""

import pytest

from etl_framework.utils.s3 import S3Options, is_s3_path, to_s3a


@pytest.mark.parametrize(
    "path,expected",
    [
        ("s3://bucket/key", True),
        ("s3a://bucket/key", True),
        ("s3n://bucket/key", True),
        ("S3://Bucket/Key", True),
        ("hdfs://ns/key", False),
        ("/local/path", False),
        ("file:///tmp/x", False),
    ],
)
def test_is_s3_path(path, expected):
    assert is_s3_path(path) is expected


@pytest.mark.parametrize(
    "path,expected",
    [
        ("s3://bucket/key", "s3a://bucket/key"),
        ("s3n://bucket/key", "s3a://bucket/key"),
        ("s3a://bucket/key", "s3a://bucket/key"),  # already s3a, unchanged
        ("hdfs://ns/key", "hdfs://ns/key"),  # non-s3, unchanged
    ],
)
def test_to_s3a(path, expected):
    assert to_s3a(path) == expected


def test_from_env_reads_standard_aws_vars():
    opts = S3Options.from_env(
        {
            "AWS_ACCESS_KEY_ID": "AK",
            "AWS_SECRET_ACCESS_KEY": "SK",
            "AWS_SESSION_TOKEN": "ST",
            "AWS_DEFAULT_REGION": "eu-west-1",
            "AWS_S3_PATH_STYLE_ACCESS": "true",
        }
    )
    assert opts.access_key == "AK"
    assert opts.secret_key == "SK"
    assert opts.session_token == "ST"
    assert opts.region == "eu-west-1"
    assert opts.path_style_access is True


def test_hadoop_conf_includes_only_set_values():
    # No static credentials -> nothing that would clobber an IAM role chain.
    assert S3Options().hadoop_conf() == {}


def test_hadoop_conf_full():
    conf = S3Options(
        access_key="AK",
        secret_key="SK",
        session_token="ST",
        region="us-east-1",
        endpoint="https://minio.local:9000",
        path_style_access=True,
        sse_algorithm="SSE-KMS",
        sse_kms_key_id="key-123",
        extra={"connection.maximum": "50", "fs.s3a.attempts.maximum": "3"},
    ).hadoop_conf()

    assert conf["fs.s3a.access.key"] == "AK"
    assert conf["fs.s3a.secret.key"] == "SK"
    assert conf["fs.s3a.session.token"] == "ST"
    # A session token switches to the temporary-credentials provider.
    assert (
        conf["fs.s3a.aws.credentials.provider"]
        == "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    )
    assert conf["fs.s3a.endpoint.region"] == "us-east-1"
    assert conf["fs.s3a.endpoint"] == "https://minio.local:9000"
    assert conf["fs.s3a.path.style.access"] == "true"
    assert conf["fs.s3a.server-side-encryption-algorithm"] == "SSE-KMS"
    assert conf["fs.s3a.server-side-encryption.key"] == "key-123"
    # `extra` keys are prefixed only when they lack the fs.s3a. prefix.
    assert conf["fs.s3a.connection.maximum"] == "50"
    assert conf["fs.s3a.attempts.maximum"] == "3"
