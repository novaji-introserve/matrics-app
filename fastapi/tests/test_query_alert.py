from jobs.query_alert import _build_email_body, _load_email_css_rules


def test_build_email_body_uses_dynamic_css_from_settings(monkeypatch, tmp_path):
    css_file = tmp_path / "query_alert.css"
    css_file.write_text(".query-alert { color: #123456; }", encoding="utf-8")

    class StubSettings:
        alert_email_css_path = str(css_file)
        alert_email_css = ""
        alert_email_logo_url = "https://example.com/logo.png"

    monkeypatch.setattr("jobs.query_alert.load_settings", lambda refresh=True: StubSettings())

    body = _build_email_body(
        job_id="aml-alert",
        name="AML Alert",
        rows=[{"customer_name": "John", "amount": 10}],
        query="select 1",
    )

    assert body.startswith("<!DOCTYPE html><html><head><meta charset='utf-8'><style>")
    assert "<style>.query-alert { color: #123456; }</style>" in body
    assert "<body><div class='query-alert'>" in body
    assert "class='query-alert__logo'" in body
    assert "src='https://example.com/logo.png'" in body
    assert "class='query-alert__table'" in body
    assert "Customer Name" in body
    assert "style=" not in body


def test_load_email_css_rules_uses_inline_css_when_path_missing(monkeypatch):
    class StubSettings:
        alert_email_css_path = "/tmp/does-not-exist.css"
        alert_email_css = ".query-alert { color: #abcdef; }"
        alert_email_logo_url = ""

    monkeypatch.setattr("jobs.query_alert.load_settings", lambda refresh=True: StubSettings())

    assert _load_email_css_rules() == ".query-alert { color: #abcdef; }"
