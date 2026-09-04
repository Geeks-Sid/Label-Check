import os
import re
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

import app as app_module


class SecurityConfigurationTests(unittest.TestCase):
    def setUp(self):
        self._original_app_test_config = {
            "TESTING": app_module.app.config.get("TESTING"),
            "SECRET_KEY": app_module.app.config.get("SECRET_KEY"),
        }
        app_module.app.config.update(
            TESTING=True,
            SECRET_KEY="security-tests-secret-key",
        )

    def tearDown(self):
        app_module.app.config.update(self._original_app_test_config)

    def valid_config(self):
        return {
            "SECRET_KEY": "s" * 32,
            "ADMIN_DEFAULT_PASSWORD": "strong-admin-password",
            "PIPELINE_INPUT_ROOTS": ("/input",),
            "PIPELINE_OUTPUT_ROOTS": ("/output",),
            "PIPELINE_MAX_WORKERS": 8,
            "PIPELINE_MAX_THUMBNAIL_DIMENSION": 4096,
            "LOGIN_PAIR_ATTEMPT_LIMIT": 5,
            "LOGIN_ACCOUNT_ATTEMPT_LIMIT": 10,
            "LOGIN_RATE_WINDOW_SECONDS": 900,
        }

    def test_valid_credentials_are_accepted(self):
        app_module.validate_security_config(self.valid_config())

    def test_compose_exposes_only_https_proxy(self):
        compose = (SRC_DIR.parent / "compose.yaml").read_text(encoding="utf-8")
        caddyfile = (SRC_DIR.parent / "container" / "caddy" / "Caddyfile").read_text(
            encoding="utf-8"
        )
        label_check_service = compose.split("  label-check:", 1)[1].split(
            "\n  caddy:", 1
        )[0]
        caddy_service = compose.split("  caddy:", 1)[1].split("\nvolumes:", 1)[0]

        self.assertNotIn('"127.0.0.1:${HOST_PORT:-5000}:5000"', compose)
        self.assertNotIn("ports:", label_check_service)
        self.assertIn('expose:\n      - "5000"', label_check_service)
        self.assertIn('- "443:443"', caddy_service)
        self.assertIn(
            "LABEL_CHECK_HOSTNAME: ${LABEL_CHECK_HOSTNAME:?Set LABEL_CHECK_HOSTNAME}",
            caddy_service,
        )
        self.assertIn("condition: service_healthy", compose)
        self.assertIn("label-check-caddy-data", compose)
        self.assertIn("https://{$LABEL_CHECK_HOSTNAME}", caddyfile)
        self.assertIn("tls internal", caddyfile)
        self.assertIn("reverse_proxy label-check:5000", caddyfile)

    def test_missing_and_short_credentials_are_rejected_without_values(self):
        cases = [
            ({}, "SECRET_KEY"),
            (
                {"SECRET_KEY": "short", "ADMIN_DEFAULT_PASSWORD": "short"},
                "SECRET_KEY",
            ),
        ]
        for configuration, expected_name in cases:
            with self.subTest(configuration=configuration):
                with self.assertRaises(app_module.SecurityConfigurationError) as raised:
                    app_module.validate_security_config(configuration)
                message = str(raised.exception)
                self.assertIn(expected_name, message)
                self.assertNotIn("short", message)

    def test_documented_and_legacy_defaults_are_rejected(self):
        for key, value in (
            ("SECRET_KEY", "a-super-secret-key-that-you-should-change"),
            ("SECRET_KEY", "replace-with-a-long-random-value"),
            ("ADMIN_DEFAULT_PASSWORD", "change_this_password"),
            ("ADMIN_DEFAULT_PASSWORD", "replace-before-first-start"),
        ):
            configuration = self.valid_config()
            configuration[key] = value
            with self.subTest(key=key, value=value):
                with self.assertRaisesRegex(
                    app_module.SecurityConfigurationError, key
                ):
                    app_module.validate_security_config(configuration)

    def test_production_request_fails_closed_when_credentials_are_invalid(self):
        original = {
            "TESTING": app_module.app.config.get("TESTING"),
            "SECRET_KEY": app_module.app.config.get("SECRET_KEY"),
            "ADMIN_DEFAULT_PASSWORD": app_module.app.config.get(
                "ADMIN_DEFAULT_PASSWORD"
            ),
        }
        try:
            app_module.app.config.update(
                TESTING=False,
                SECRET_KEY="short",
                ADMIN_DEFAULT_PASSWORD="short",
            )
            response = app_module.app.test_client().get("/login")
            self.assertEqual(500, response.status_code)
            self.assertEqual(
                b"Server security configuration is invalid.", response.data
            )
        finally:
            app_module.app.config.update(original)

    def test_init_db_does_not_create_admin_with_invalid_credentials(self):
        original = {
            "SECRET_KEY": app_module.app.config.get("SECRET_KEY"),
            "ADMIN_DEFAULT_PASSWORD": app_module.app.config.get(
                "ADMIN_DEFAULT_PASSWORD"
            ),
        }
        try:
            app_module.app.config.update(
                SECRET_KEY="short",
                ADMIN_DEFAULT_PASSWORD="short",
            )
            with mock.patch.object(app_module.user_manager, "add") as add_user:
                result = app_module.app.test_cli_runner().invoke(args=["init-db"])
            self.assertNotEqual(0, result.exit_code)
            self.assertIn("SECRET_KEY", result.output)
            add_user.assert_not_called()
        finally:
            app_module.app.config.update(original)

    def test_runtime_permissions_are_repaired_and_symlinks_rejected(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            instance = root / "instance"
            logs = root / "logs"
            output = instance / "pipeline_job_output"
            output.mkdir(parents=True)
            logs.mkdir()
            sensitive = instance / "users.csv"
            sensitive.write_text("password_hash\n", encoding="utf-8")
            app_log = logs / "app.log"
            app_log.write_text("sensitive path\n", encoding="utf-8")
            os.chmod(instance, 0o755)
            os.chmod(output, 0o755)
            os.chmod(sensitive, 0o644)
            os.chmod(logs, 0o755)
            os.chmod(app_log, 0o644)

            app_module.harden_runtime_permissions(instance, logs)

            self.assertEqual(0o700, stat.S_IMODE(instance.stat().st_mode))
            self.assertEqual(0o700, stat.S_IMODE(output.stat().st_mode))
            self.assertEqual(0o600, stat.S_IMODE(sensitive.stat().st_mode))
            self.assertEqual(0o600, stat.S_IMODE(app_log.stat().st_mode))

            linked = instance / "linked-state"
            try:
                linked.symlink_to(root, target_is_directory=True)
            except OSError:
                self.skipTest("Directory symlinks are unavailable")
            with self.assertRaisesRegex(RuntimeError, "symbolic link"):
                app_module.harden_runtime_permissions(instance, logs)

    def test_container_bind_mount_can_rely_on_windows_acls_when_chmod_is_unsupported(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "batch_catalog.sqlite3"
            path.touch()
            permission_error = PermissionError(1, "Operation not permitted", str(path))
            with (
                mock.patch.dict(os.environ, {"LABEL_CHECK_CONTAINER": "true"}),
                mock.patch.object(os, "chmod", side_effect=permission_error),
                mock.patch.object(os, "access", return_value=True),
            ):
                self.assertFalse(app_module._set_private_mode(path, 0o600))

            with (
                mock.patch.dict(os.environ, {"LABEL_CHECK_CONTAINER": "false"}),
                mock.patch.object(os, "chmod", side_effect=permission_error),
            ):
                with self.assertRaises(PermissionError):
                    app_module._set_private_mode(path, 0o600)

    def test_api_store_creates_private_database_and_job_output(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            store = app_module.APIStore(
                str(root / "state" / "api.sqlite3"),
                str(root / "state" / "pipeline-output"),
            )
            output_path = Path(
                store.reserve_job("job", "owner", {}, ["pipeline"])
            )

            self.assertEqual(
                0o600, stat.S_IMODE((root / "state" / "api.sqlite3").stat().st_mode)
            )
            self.assertEqual(0o600, stat.S_IMODE(output_path.stat().st_mode))
            self.assertEqual(0o700, stat.S_IMODE(output_path.parent.stat().st_mode))

    def test_password_policy_enforces_length_without_trimming(self):
        self.assertIsNotNone(app_module.password_policy_error("x" * 11))
        self.assertIsNone(app_module.password_policy_error(" " + "x" * 11))
        self.assertIsNone(app_module.password_policy_error("x" * 128))
        self.assertIsNotNone(app_module.password_policy_error("x" * 129))
        self.assertIsNotNone(
            app_module.password_policy_error("replace-before-first-start")
        )

    def test_login_throttle_is_durable_and_success_clears_failures(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            original_store = (
                app_module.api_store.db_path,
                app_module.api_store.output_dir,
            )
            original_users = app_module.user_manager.users.copy()
            original_config = {
                key: app_module.app.config.get(key)
                for key in (
                    "TESTING",
                    "SECRET_KEY",
                    "LOGIN_PAIR_ATTEMPT_LIMIT",
                    "LOGIN_ACCOUNT_ATTEMPT_LIMIT",
                    "LOGIN_RATE_WINDOW_SECONDS",
                )
            }
            try:
                database = str(root / "api.sqlite3")
                output = str(root / "output")
                app_module.api_store.configure(database, output)
                user = app_module.User("login-user", "")
                user.set_password("valid-password-123")
                app_module.user_manager.users[user.id] = user
                app_module.app.config.update(
                    TESTING=True,
                    SECRET_KEY="login-test-secret",
                    LOGIN_PAIR_ATTEMPT_LIMIT=2,
                    LOGIN_ACCOUNT_ATTEMPT_LIMIT=3,
                    LOGIN_RATE_WINDOW_SECONDS=900,
                )
                client = app_module.app.test_client()

                first = client.post(
                    "/login",
                    data={"username": user.id, "password": "wrong"},
                    environ_base={"REMOTE_ADDR": "192.0.2.10"},
                )
                second = client.post(
                    "/login",
                    data={"username": user.id, "password": "wrong"},
                    environ_base={"REMOTE_ADDR": "192.0.2.10"},
                )
                app_module.api_store.configure(database, output)
                after_restart = client.post(
                    "/login",
                    data={"username": user.id, "password": "wrong"},
                    environ_base={"REMOTE_ADDR": "192.0.2.10"},
                )

                self.assertEqual(200, first.status_code)
                self.assertEqual(429, second.status_code)
                self.assertIn("Retry-After", second.headers)
                self.assertEqual(429, after_restart.status_code)

                app_module.api_store.clear_login_failures(user.id, "192.0.2.10")
                failed_once = client.post(
                    "/login",
                    data={"username": user.id, "password": "wrong"},
                    environ_base={"REMOTE_ADDR": "192.0.2.10"},
                )
                succeeded = client.post(
                    "/login",
                    data={"username": user.id, "password": "valid-password-123"},
                    environ_base={"REMOTE_ADDR": "192.0.2.10"},
                )
                allowed, _retry = app_module.api_store.login_rate_limit(
                    user.id, "192.0.2.10", 1, 1, 900
                )
                self.assertEqual(200, failed_once.status_code)
                self.assertEqual(302, succeeded.status_code)
                self.assertTrue(allowed)
            finally:
                app_module.app.config.update(original_config)
                app_module.user_manager.users = original_users
                app_module.api_store.configure(*original_store)

    def test_login_redirect_accepts_only_root_relative_application_paths(self):
        original_users = app_module.user_manager.users.copy()
        user = app_module.User("redirect-user", "")
        user.set_password("valid-password-123")
        app_module.user_manager.users[user.id] = user
        try:
            cases = (
                ("/qc?batch=case-1#slide", "/qc?batch=case-1#slide"),
                ("https://attacker.example/phish", "/"),
                ("//attacker.example/phish", "/"),
                ("javascript:alert(1)", "/"),
                ("/\\attacker.example/phish", "/"),
                ("/safe\nLocation: https://attacker.example", "/"),
            )
            for supplied, expected in cases:
                with self.subTest(next=supplied):
                    response = app_module.app.test_client().post(
                        "/login",
                        data={
                            "username": user.id,
                            "password": "valid-password-123",
                            "next": supplied,
                        },
                    )
                    self.assertEqual(302, response.status_code)
                    self.assertEqual(expected, response.headers["Location"])
        finally:
            app_module.user_manager.users = original_users

    def test_login_form_preserves_only_safe_next_value(self):
        safe_response = app_module.app.test_client().get("/login?next=/qc%3Fbatch%3Done")
        unsafe_response = app_module.app.test_client().get(
            "/login?next=https://attacker.example"
        )

        self.assertIn(b'name="next" value="/qc?batch=one"', safe_response.data)
        self.assertNotIn(b'name="next"', unsafe_response.data)

    def test_browser_security_headers_and_csp_nonces(self):
        response = app_module.app.test_client().get(
            "/login", base_url="https://label-check.example"
        )

        policy = response.headers["Content-Security-Policy"]
        nonce_match = re.search(r"script-src 'self' 'nonce-([^']+)'", policy)
        self.assertIsNotNone(nonce_match)
        nonce = nonce_match.group(1)
        self.assertIn(f"style-src 'self' 'nonce-{nonce}'", policy)
        self.assertIn(f'<style nonce="{nonce}">'.encode(), response.data)
        self.assertEqual("DENY", response.headers["X-Frame-Options"])
        self.assertEqual("nosniff", response.headers["X-Content-Type-Options"])
        self.assertEqual(
            "strict-origin-when-cross-origin", response.headers["Referrer-Policy"]
        )
        self.assertEqual(
            "max-age=31536000", response.headers["Strict-Transport-Security"]
        )

        plain_response = app_module.app.test_client().get("/missing")
        self.assertNotIn("Strict-Transport-Security", plain_response.headers)
        self.assertIn("Content-Security-Policy", plain_response.headers)

    def test_session_cookie_is_secure_by_default_with_development_override(self):
        original_users = app_module.user_manager.users.copy()
        original_secure = app_module.app.config["SESSION_COOKIE_SECURE"]
        user = app_module.User("cookie-user", "")
        user.set_password("valid-password-123")
        app_module.user_manager.users[user.id] = user
        try:
            secure_response = app_module.app.test_client().post(
                "/login",
                data={"username": user.id, "password": "valid-password-123"},
                base_url="https://label-check.example",
            )
            secure_cookie = secure_response.headers["Set-Cookie"]
            self.assertIn("Secure", secure_cookie)
            self.assertIn("HttpOnly", secure_cookie)
            self.assertIn("SameSite=Lax", secure_cookie)

            app_module.app.config["SESSION_COOKIE_SECURE"] = False
            development_response = app_module.app.test_client().post(
                "/login",
                data={"username": user.id, "password": "valid-password-123"},
            )
            self.assertNotIn("; Secure", development_response.headers["Set-Cookie"])
        finally:
            app_module.app.config["SESSION_COOKIE_SECURE"] = original_secure
            app_module.user_manager.users = original_users

    def test_templates_do_not_contain_csp_bypasses_or_external_fonts(self):
        for template in (SRC_DIR / "templates").glob("*.html"):
            contents = template.read_text(encoding="utf-8")
            with self.subTest(template=template.name):
                self.assertNotIn("fonts.googleapis.com", contents)
                self.assertNotRegex(contents, r"\sstyle=")
                self.assertNotRegex(contents, r"\son(?:click|change|load)=")
                self.assertNotRegex(contents, r"<script(?! nonce=)")
                self.assertNotRegex(contents, r"<style(?! nonce=)")

    def test_login_account_limit_combines_failures_across_addresses(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            store = app_module.APIStore(
                str(root / "api.sqlite3"), str(root / "output")
            )
            for client_address in ("192.0.2.1", "192.0.2.2", "192.0.2.3"):
                store.record_login_failure("target-user", client_address, 900)

            allowed, retry_after = store.login_rate_limit(
                "target-user", "192.0.2.4", 100, 3, 900
            )

            self.assertFalse(allowed)
            self.assertGreater(retry_after, 0)


if __name__ == "__main__":
    unittest.main()
