"""Tests for the auth module — password hashing, user management, and RBAC."""
import os
import pytest
from pathlib import Path


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Fresh DB for each test + reset env-var admin credentials."""
    db_file = str(tmp_path / "auth_test.db")
    monkeypatch.setenv("DATABASE_PATH", db_file)
    monkeypatch.setenv("DATAPLATFORM_USERNAME", "admin")
    monkeypatch.setenv("DATAPLATFORM_PASSWORD", "admin")
    import dataplatform.core.database as db_module
    db_module._initialized = False
    db_module._DB_PATH = Path(db_file)
    yield
    db_module._initialized = False


from dataplatform.core.auth import (
    check_password,
    create_user,
    delete_user,
    get_all_users,
    has_permission,
    make_password_hash,
    update_user_role,
    verify_user,
)


class TestPasswordHashing:
    def test_hash_and_check_roundtrip(self):
        h = make_password_hash("s3cr3t")
        assert check_password("s3cr3t", h) is True

    def test_wrong_password_fails(self):
        h = make_password_hash("correct")
        assert check_password("wrong", h) is False

    def test_hashes_differ_per_call(self):
        h1 = make_password_hash("same")
        h2 = make_password_hash("same")
        assert h1 != h2  # different salts

    def test_malformed_hash_returns_false(self):
        assert check_password("anything", "nosalthere") is False


class TestVerifyUser:
    def test_env_admin_authenticates(self):
        user = verify_user("admin", "admin")
        assert user is not None
        assert user["role"] == "admin"

    def test_env_admin_wrong_password(self):
        assert verify_user("admin", "wrong") is None

    def test_db_user_authenticates(self):
        create_user("alice", "pass123", role="editor")
        user = verify_user("alice", "pass123")
        assert user is not None
        assert user["role"] == "editor"

    def test_db_user_wrong_password(self):
        create_user("bob", "correct")
        assert verify_user("bob", "incorrect") is None

    def test_nonexistent_user_returns_none(self):
        assert verify_user("nobody", "x") is None


class TestCreateUser:
    def test_creates_user_successfully(self):
        assert create_user("carol", "pw", role="viewer") is True

    def test_duplicate_username_returns_false(self):
        create_user("dave", "pw")
        assert create_user("dave", "pw") is False

    def test_invalid_role_raises(self):
        with pytest.raises(ValueError, match="Invalid role"):
            create_user("eve", "pw", role="superuser")

    def test_cannot_use_env_admin_name(self, monkeypatch):
        monkeypatch.setenv("DATAPLATFORM_USERNAME", "sysadmin")
        with pytest.raises(ValueError, match="env-var admin"):
            create_user("sysadmin", "pw", role="admin")

    def test_team_stored(self):
        create_user("frank", "pw", role="editor", team="analytics")
        from dataplatform.core.database import get_user
        user = get_user("frank")
        assert user["team"] == "analytics"


class TestUpdateUserRole:
    def test_updates_role(self):
        create_user("grace", "pw", role="viewer")
        ok = update_user_role("grace", "editor")
        assert ok is True
        from dataplatform.core.database import get_user
        assert get_user("grace")["role"] == "editor"

    def test_invalid_role_raises(self):
        create_user("henry", "pw", role="viewer")
        with pytest.raises(ValueError, match="Invalid role"):
            update_user_role("henry", "god")

    def test_nonexistent_user_returns_false(self):
        assert update_user_role("nobody", "admin") is False


class TestDeleteUser:
    def test_deletes_user(self):
        create_user("ivan", "pw")
        assert delete_user("ivan") is True

    def test_nonexistent_returns_false(self):
        assert delete_user("nobody") is False

    def test_cannot_delete_env_admin(self, monkeypatch):
        monkeypatch.setenv("DATAPLATFORM_USERNAME", "boss")
        with pytest.raises(ValueError, match="environment-variable admin"):
            delete_user("boss")


class TestGetAllUsers:
    def test_returns_list(self):
        create_user("judy", "pw", role="viewer")
        users = get_all_users()
        names = [u["username"] for u in users]
        assert "judy" in names

    def test_no_password_hashes(self):
        create_user("kate", "pw")
        for u in get_all_users():
            assert "password_hash" not in u


class TestHasPermission:
    def test_admin_has_wildcard(self):
        assert has_permission("admin", "run") is True
        assert has_permission("admin", "anything") is True

    def test_editor_can_run(self):
        assert has_permission("editor", "run") is True
        assert has_permission("editor", "schedule") is True
        assert has_permission("editor", "read") is True

    def test_editor_cannot_manage_users(self):
        assert has_permission("editor", "*") is False

    def test_viewer_can_read(self):
        assert has_permission("viewer", "read") is True

    def test_viewer_cannot_run(self):
        assert has_permission("viewer", "run") is False

    def test_unknown_role_has_no_permissions(self):
        assert has_permission("ghost", "read") is False
