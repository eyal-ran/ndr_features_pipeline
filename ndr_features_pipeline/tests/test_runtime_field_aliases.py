from ndr.runtime_field_aliases import resolve_with_legacy_alias


def test_resolve_with_legacy_alias_prefers_canonical(caplog):
    with caplog.at_level("WARNING"):
        resolved = resolve_with_legacy_alias(
            canonical_value="s3://bucket/canonical/",
            legacy_value="s3://bucket/legacy/",
            canonical_name="raw_parsed_logs_s3_prefix",
            legacy_name="mini_batch_s3_prefix",
            context="test",
        )
    assert resolved == "s3://bucket/canonical/"
    assert "LegacyFieldNameUsed" not in caplog.text


def test_resolve_with_legacy_alias_logs_deterministic_warning(caplog):
    with caplog.at_level("WARNING"):
        resolved = resolve_with_legacy_alias(
            canonical_value="",
            legacy_value="s3://bucket/legacy/",
            canonical_name="raw_parsed_logs_s3_prefix",
            legacy_name="mini_batch_s3_prefix",
            context="test",
        )
    assert resolved == "s3://bucket/legacy/"
    assert (
        "LegacyFieldNameUsed context=test "
        "legacy_field=mini_batch_s3_prefix canonical_field=raw_parsed_logs_s3_prefix"
    ) in caplog.text
