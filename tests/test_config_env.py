"""Tests for ${ENV_VAR} interpolation in config loading."""

import pytest

from nerve.config import ConfigError, _resolve_env_refs, load_config


class TestResolveEnvRefs:
    def test_required_ref_resolves_from_env(self, monkeypatch):
        monkeypatch.setenv("NERVE_X", "resolved")
        assert _resolve_env_refs({"k": "${NERVE_X}"}) == {"k": "resolved"}

    def test_ref_embedded_in_larger_string(self, monkeypatch):
        monkeypatch.setenv("HOST", "db.internal")
        out = _resolve_env_refs({"url": "https://${HOST}:8443/x"})
        assert out == {"url": "https://db.internal:8443/x"}

    def test_default_used_when_unset(self, monkeypatch):
        monkeypatch.delenv("NERVE_Y", raising=False)
        assert _resolve_env_refs({"k": "${NERVE_Y:-fallback}"}) == {"k": "fallback"}

    def test_default_used_when_empty(self, monkeypatch):
        monkeypatch.setenv("NERVE_Y", "")
        assert _resolve_env_refs({"k": "${NERVE_Y:-fallback}"}) == {"k": "fallback"}

    def test_env_wins_over_default(self, monkeypatch):
        monkeypatch.setenv("NERVE_Y", "actual")
        assert _resolve_env_refs({"k": "${NERVE_Y:-fallback}"}) == {"k": "actual"}

    def test_missing_required_raises_listing_var(self, monkeypatch):
        monkeypatch.delenv("NERVE_MISSING", raising=False)
        with pytest.raises(ConfigError) as ei:
            _resolve_env_refs({"k": "${NERVE_MISSING}"})
        assert "NERVE_MISSING" in str(ei.value)

    def test_multiple_missing_all_listed(self, monkeypatch):
        monkeypatch.delenv("MISS_A", raising=False)
        monkeypatch.delenv("MISS_B", raising=False)
        with pytest.raises(ConfigError) as ei:
            _resolve_env_refs({"a": "${MISS_A}", "b": {"c": "${MISS_B}"}})
        msg = str(ei.value)
        assert "MISS_A" in msg and "MISS_B" in msg

    def test_recurses_dicts_and_lists(self, monkeypatch):
        monkeypatch.setenv("V", "x")
        out = _resolve_env_refs({"a": ["${V}", {"b": "${V}"}], "n": 5, "flag": True})
        assert out == {"a": ["x", {"b": "x"}], "n": 5, "flag": True}

    def test_bare_dollar_values_untouched(self):
        """Critical: bcrypt hashes / jwt secrets contain $ but no braces."""
        bcrypt = "$2b$12$abcdefghijklmnopqrstuv"
        out = _resolve_env_refs({"password_hash": bcrypt, "s": "cost=$5"})
        assert out == {"password_hash": bcrypt, "s": "cost=$5"}

    def test_double_dollar_escapes_to_literal(self):
        assert _resolve_env_refs({"k": "$${NOT_A_VAR}"}) == {"k": "${NOT_A_VAR}"}

    def test_non_string_leaves_pass_through(self):
        assert _resolve_env_refs({"n": 1, "b": False, "z": None}) == {
            "n": 1, "b": False, "z": None,
        }

    def test_adjacent_refs(self, monkeypatch):
        monkeypatch.setenv("A", "x")
        monkeypatch.setenv("B", "y")
        assert _resolve_env_refs({"k": "${A}${B}"}) == {"k": "xy"}

    def test_required_empty_value_accepted(self, monkeypatch):
        """${VAR} (required) checks only *unset*; VAR="" yields "" (POSIX)."""
        monkeypatch.setenv("EMPTY_VAR", "")
        assert _resolve_env_refs({"k": "${EMPTY_VAR}"}) == {"k": ""}

    def test_falsy_looking_values_kept_not_defaulted(self, monkeypatch):
        """Non-empty strings like "0"/"false"/" " are truthy → kept, not
        replaced by the :- default."""
        for val in ("0", "false", " "):
            monkeypatch.setenv("FLAG", val)
            assert _resolve_env_refs({"k": "${FLAG:-D}"}) == {"k": val}

    def test_empty_name_left_intact(self):
        assert _resolve_env_refs({"k": "a${}b"}) == {"k": "a${}b"}


class TestLoadMcpServersInterpolation:
    def test_mcp_server_env_ref_resolved(self, tmp_path, monkeypatch):
        from nerve.config import load_mcp_servers

        monkeypatch.setenv("MCP_TOKEN", "sk-mcp-secret")
        (tmp_path / "config.yaml").write_text(
            "mcp_servers:\n"
            "  demo:\n"
            "    type: http\n"
            "    url: https://example.com/mcp\n"
            "    headers:\n"
            "      Authorization: Bearer ${MCP_TOKEN}\n",
            encoding="utf-8",
        )
        servers = load_mcp_servers(tmp_path)
        demo = next(s for s in servers if s.name == "demo")
        # The secret was interpolated from the environment, not left as ${...}.
        assert "sk-mcp-secret" in str(demo.headers)
        assert "${MCP_TOKEN}" not in str(demo.headers)

    def test_mcp_missing_required_var_raises(self, tmp_path, monkeypatch):
        from nerve.config import load_mcp_servers

        monkeypatch.delenv("MCP_NOPE", raising=False)
        (tmp_path / "config.yaml").write_text(
            "mcp_servers:\n"
            "  demo:\n"
            "    type: http\n"
            "    url: https://example.com/mcp\n"
            "    headers:\n"
            "      Authorization: Bearer ${MCP_NOPE}\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigError):
            load_mcp_servers(tmp_path)


class TestCleanCliError:
    def test_missing_var_renders_clean_error_not_traceback(self, tmp_path, monkeypatch):
        from click.testing import CliRunner

        from nerve.cli import main

        monkeypatch.delenv("NOPE_CLI", raising=False)
        (tmp_path / "config.yaml").write_text(
            "anthropic_api_key: ${NOPE_CLI}\n", encoding="utf-8"
        )
        result = CliRunner().invoke(main, ["-c", str(tmp_path), "doctor"])
        assert result.exit_code != 0
        assert "NOPE_CLI" in result.output
        assert "Traceback" not in result.output


class TestLoadConfigInterpolation:
    def test_end_to_end_from_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MY_ANTHROPIC", "sk-from-env")
        (tmp_path / "config.yaml").write_text(
            "anthropic_api_key: ${MY_ANTHROPIC}\n", encoding="utf-8"
        )
        cfg = load_config(tmp_path)
        assert cfg.anthropic_api_key == "sk-from-env"

    def test_missing_required_var_fails_load(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NOPE_KEY", raising=False)
        (tmp_path / "config.yaml").write_text(
            "anthropic_api_key: ${NOPE_KEY}\n", encoding="utf-8"
        )
        with pytest.raises(ConfigError):
            load_config(tmp_path)

    def test_default_in_config_file(self, tmp_path, monkeypatch):
        # BIND_HOST is a plausible name to already have exported in a dev shell
        # or CI job; without the delenv this passes on whatever value is lying
        # around and never exercises the `:-` fallback it claims to.
        monkeypatch.delenv("BIND_HOST", raising=False)
        (tmp_path / "config.yaml").write_text(
            'gateway:\n  host: "${BIND_HOST:-127.0.0.1}"\n', encoding="utf-8"
        )
        cfg = load_config(tmp_path)
        assert cfg.gateway.host == "127.0.0.1"

    def test_env_wins_over_default_in_config_file(self, tmp_path, monkeypatch):
        """The other half of the pair: a set variable must beat the fallback.

        On its own, `test_default_in_config_file` also passes if `:-` handling
        collapses to "always take the default", which would ignore every value
        an operator actually exported.
        """
        monkeypatch.setenv("BIND_HOST", "10.0.0.5")
        (tmp_path / "config.yaml").write_text(
            'gateway:\n  host: "${BIND_HOST:-127.0.0.1}"\n', encoding="utf-8"
        )
        cfg = load_config(tmp_path)
        assert cfg.gateway.host == "10.0.0.5"

    def test_local_overlay_secret_from_env(self, tmp_path, monkeypatch):
        """config.local.yaml can reference env too, merged on top of base."""
        monkeypatch.setenv("OAI", "sk-openai-env")
        (tmp_path / "config.yaml").write_text(
            "openai_api_key: placeholder\n", encoding="utf-8"
        )
        (tmp_path / "config.local.yaml").write_text(
            "openai_api_key: ${OAI}\n", encoding="utf-8"
        )
        cfg = load_config(tmp_path)
        assert cfg.openai_api_key == "sk-openai-env"


class TestScalarCoercion:
    """`${VAR}` interpolation is a string substitution — it erases YAML types.

    `bool("false")` is True and an int field left as `"8900"` raises at first
    use, so without coercion an env-referenced flag silently means the opposite
    of what it says. This matters more under lockdown, where the machine-local
    layers are dropped and every per-machine value *has* to be an env ref.
    """

    def test_as_bool_spellings(self):
        from nerve.coerce import as_bool

        for text in ("false", "False", "  FALSE ", "0", "no", "off", "n", "f"):
            assert as_bool(text, True) is False, text
        for text in ("true", "TRUE", "1", "yes", "on", "y", "t"):
            assert as_bool(text, False) is True, text

    def test_as_bool_passes_through_real_bools(self):
        from nerve.coerce import as_bool

        assert as_bool(True, False) is True
        assert as_bool(False, True) is False

    def test_as_bool_falls_back_rather_than_enabling(self):
        """An unparseable value must never turn a feature on by accident."""
        from nerve.coerce import as_bool

        assert as_bool("maybe", False) is False
        assert as_bool("maybe", True) is True
        assert as_bool([], False) is False
        assert as_bool([], True) is True

    def test_as_bool_null_and_empty_are_off(self):
        """Not "fall back to the default" — these were falsy before coercion,
        and `FLAG=` must stay a working way to switch something off."""
        from nerve.coerce import as_bool

        assert as_bool(None, True) is False
        assert as_bool("", True) is False
        assert as_bool("   ", True) is False

    def test_as_bool_numeric(self):
        from nerve.coerce import as_bool

        assert as_bool(1, False) is True
        assert as_bool(0, True) is False

    def test_lenient_coercers_fall_back_on_junk(self):
        from nerve.coerce import lenient_float, lenient_int

        assert lenient_int("8900", 1) == 8900
        assert lenient_int("junk", 7) == 7
        assert lenient_int(None, 7) == 7
        assert lenient_float("1.5", 0.0) == 1.5
        assert lenient_float("junk", 2.5) == 2.5

    def test_env_ref_on_bool_field_end_to_end(self, tmp_path, monkeypatch):
        """The headline case: a flag disabled via env must actually be off."""
        monkeypatch.setenv("MCP_ON", "false")
        (tmp_path / "config.yaml").write_text(
            "mcp_endpoint:\n  enabled: ${MCP_ON}\n", encoding="utf-8"
        )
        assert load_config(tmp_path).mcp_endpoint.enabled is False

    def test_env_ref_on_int_field_end_to_end(self, tmp_path, monkeypatch):
        """`port: ${PORT}` is the literal example in docs/config.md."""
        monkeypatch.setenv("PORT", "9001")
        (tmp_path / "config.yaml").write_text(
            "gateway:\n  port: ${PORT}\n", encoding="utf-8"
        )
        port = load_config(tmp_path).gateway.port
        assert port == 9001 and isinstance(port, int)

    def test_quoted_yaml_bool_without_any_env_var(self, tmp_path):
        """Reachable with no interpolation at all — just a quoted scalar."""
        (tmp_path / "config.yaml").write_text(
            'retention:\n  enabled: "false"\n', encoding="utf-8"
        )
        assert load_config(tmp_path).retention.enabled is False

    def test_null_and_empty_are_off_not_default(self, tmp_path):
        """A bare `key:` and an empty ${VAR} must mean off, not "keep default".

        Both were falsy before coercion existed (`bool(None)`, `bool("")`).
        Returning the field's default instead would silently *enable* the ten
        bool fields whose default is True — including
        agent.background_agent_permissions, which grants a catch-all
        permission hook.
        """
        (tmp_path / "config.yaml").write_text(
            "agent:\n"
            "  background_agent_permissions:\n"
            "backup:\n"
            "  include_workspace:\n"
            "  notify_on_failure:\n",
            encoding="utf-8",
        )
        cfg = load_config(tmp_path)
        assert cfg.agent.background_agent_permissions is False
        assert cfg.backup.include_workspace is False
        assert cfg.backup.notify_on_failure is False

    def test_empty_env_var_disables(self, tmp_path, monkeypatch):
        """`FLAG=` is the natural "off" spelling when the value must be a ref."""
        monkeypatch.setenv("FLAG", "")
        (tmp_path / "config.yaml").write_text(
            "backup:\n  include_workspace: ${FLAG}\n", encoding="utf-8"
        )
        assert load_config(tmp_path).backup.include_workspace is False

    def test_optional_int_field_is_coerced(self, tmp_path, monkeypatch):
        """`int | None` counts too — None stays None, a string becomes int."""
        monkeypatch.setenv("TGC", "-1001")
        (tmp_path / "config.yaml").write_text(
            "notifications:\n  telegram_chat_id: ${TGC}\n", encoding="utf-8"
        )
        assert load_config(tmp_path).notifications.telegram_chat_id == -1001

    def test_list_of_int_elements_are_coerced(self, tmp_path, monkeypatch):
        """sources/telegram.py compares against these with `in` — a str entry
        never matches, so the chat you meant to exclude gets synced anyway."""
        monkeypatch.setenv("CHAT", "-1001")
        (tmp_path / "config.yaml").write_text(
            'sync:\n  telegram:\n    exclude_chats: ["${CHAT}", 42]\n',
            encoding="utf-8",
        )
        assert load_config(tmp_path).sync.telegram.exclude_chats == [-1001, 42]

    def test_unconvertible_list_element_is_kept_not_dropped(self, tmp_path):
        """Silently shrinking an allowlist/denylist is worse than a loud type."""
        (tmp_path / "config.yaml").write_text(
            'sync:\n  telegram:\n    exclude_chats: [7, "nope"]\n', encoding="utf-8"
        )
        assert load_config(tmp_path).sync.telegram.exclude_chats == [7, "nope"]

    def test_bad_allowed_user_does_not_stop_the_daemon_booting(
        self, tmp_path, monkeypatch,
    ):
        """An unresolvable ref in an allowlist must degrade, not crash.

        telegram may not even be enabled on this host, and a config that
        refuses to load takes down every other subsystem with it.
        """
        monkeypatch.setenv("TG_USERS", "not-a-number")
        (tmp_path / "config.yaml").write_text(
            'telegram:\n  allowed_users: ["${TG_USERS}", "8900"]\n',
            encoding="utf-8",
        )
        cfg = load_config(tmp_path)
        # Kept verbatim so it stands out, and 8900 still resolves. The set()
        # the Telegram channel builds from this holds a str that no int user
        # id can match, so the bad entry fails closed.
        assert cfg.telegram.allowed_users == ["not-a-number", 8900]

    def test_a_bare_string_allowlist_is_not_read_character_by_character(
        self, tmp_path,
    ):
        """`allowed_users: "123"` must not authorize users 1, 2 and 3."""
        (tmp_path / "config.yaml").write_text(
            'telegram:\n  allowed_users: "123"\n', encoding="utf-8"
        )
        users = load_config(tmp_path).telegram.allowed_users
        assert users != [1, 2, 3]
        # Widened to one entry rather than left as a str: leaving the string
        # alone only moves the character-splitting to the consumers, which all
        # call set() on this.
        assert users == [123]

    def test_comma_separated_env_ref_becomes_a_list(self, tmp_path, monkeypatch):
        """The only way to spell a multi-value list in a single env var.

        Lockdown discards the machine-local layers, so a per-machine value like
        an allowlist *has* to be a ${VAR} reference in the tracked settings —
        and interpolation hands the builder one flat string.
        """
        monkeypatch.setenv("TG_USERS", " 123 , 456 ")
        (tmp_path / "config.yaml").write_text(
            "telegram:\n  allowed_users: ${TG_USERS}\n", encoding="utf-8"
        )
        assert load_config(tmp_path).telegram.allowed_users == [123, 456]

    def test_blank_list_ref_is_empty_not_one_empty_entry(self, tmp_path, monkeypatch):
        """An empty allowlist is what re-arms Telegram pairing mode.

        A single blank entry would leave the list truthy, and
        _is_authorized's `if not self._allowed_users` gate would never fire.
        """
        monkeypatch.setenv("TG_USERS", "")
        (tmp_path / "config.yaml").write_text(
            "telegram:\n  allowed_users: ${TG_USERS}\n", encoding="utf-8"
        )
        assert load_config(tmp_path).telegram.allowed_users == []

    def test_lone_scalar_on_a_list_field_is_wrapped(self, tmp_path):
        """`exclude_chats: -1001` is an easy thing to write and mean."""
        (tmp_path / "config.yaml").write_text(
            "sync:\n  telegram:\n    exclude_chats: -1001\n", encoding="utf-8"
        )
        assert load_config(tmp_path).sync.telegram.exclude_chats == [-1001]

    def test_string_exclude_chats_actually_excludes(self, tmp_path, monkeypatch):
        """The fail-*open* half, reproduced the way the source consumes it.

        sources/telegram.py builds `set(exclude_chats)` and tests an int chat
        id for membership. Against the raw string "-1001" that set is a bag of
        single characters, nothing matches, and the excluded chat is synced.
        """
        monkeypatch.setenv("SKIP", "-1001,-1002")
        (tmp_path / "config.yaml").write_text(
            "sync:\n  telegram:\n    exclude_chats: ${SKIP}\n", encoding="utf-8"
        )
        excluded = set(load_config(tmp_path).sync.telegram.exclude_chats)
        assert -1001 in excluded
        assert -1002 in excluded

    def test_non_scalar_on_a_list_field_is_left_alone(self, tmp_path):
        """A mapping isn't a list spelled oddly — don't invent a reading."""
        (tmp_path / "config.yaml").write_text(
            "telegram:\n  allowed_users:\n    nope: 1\n", encoding="utf-8"
        )
        assert load_config(tmp_path).telegram.allowed_users == {"nope": 1}

    def test_every_declared_scalar_survives_a_string_value(self):
        """Sweep the whole dataclass tree so a new field can't reintroduce this.

        A builder that eagerly casts (`bool(d.get("enabled", True))`) defeats
        @coerced, because bool("false") is already True by the time the
        decorator sees it. This catches that, and catches a new config
        dataclass whose from_dict forgets the decorator.

        Scope, so the green tick isn't read as more than it is: only
        dataclasses reachable as attributes of `nerve.config` with a callable
        `from_dict`. Values read straight off the merged dict (e.g. the
        top-level `lockdown` flag) and dataclasses in other modules that
        nerve.config doesn't re-export are NOT covered here.
        """
        import dataclasses
        import inspect
        import typing

        import nerve.config as cfg
        from nerve.coerce import _classify

        probes = {
            bool: [("false", False), ("true", True), ("0", False), ("on", True)],
            int: [("8900", 8900)],
            float: [("1.5", 1.5)],
            # Only ever reached as list[str] — a bare str field needs no
            # coercion, so _classify leaves it alone. The value is deliberately
            # several characters long and contains a comma, because both of the
            # ways a string can be mistaken for a collection (iterating it,
            # splitting it) turn this into more than one entry.
            str: [("alpha,beta", "alpha,beta")],
        }
        two_arg = {"McpServerConfig"}  # from_dict(cls, name, d)
        broken, checked = [], 0
        for _name, klass in sorted(vars(cfg).items()):
            if not (inspect.isclass(klass) and dataclasses.is_dataclass(klass)):
                continue
            from_dict = getattr(klass, "from_dict", None)
            if not callable(from_dict):
                continue
            hints = typing.get_type_hints(klass)
            for f in dataclasses.fields(klass):
                classified = _classify(hints.get(f.name))
                if classified is None:
                    continue
                kind, base = classified
                checked += 1
                for raw, want in probes[base]:
                    value = [raw] if kind == "list" else raw
                    expected = [want] if kind == "list" else want
                    args = ("probe", {f.name: value}) if klass.__name__ in two_arg \
                        else ({f.name: value},)
                    got = getattr(from_dict(*args), f.name)
                    if got != expected or (
                        kind != "list" and type(got) is not base
                    ):
                        broken.append(
                            f"{klass.__name__}.{f.name} ({kind} {base.__name__}): "
                            f"{value!r} -> {type(got).__name__}={got!r}"
                        )
                    # A ${VAR} reference on a list field arrives as a bare
                    # scalar, not a one-element list, and it has to become
                    # exactly one element. This is the probe that separates a
                    # wrap from a widen: `list("alpha,beta")` is ten entries and
                    # a comma split is two, and once either has happened nothing
                    # downstream can tell the result from a genuine list.
                    if kind == "list":
                        args = ("probe", {f.name: raw}) if klass.__name__ in two_arg \
                            else ({f.name: raw},)
                        got = getattr(from_dict(*args), f.name)
                        if got != [want]:
                            broken.append(
                                f"{klass.__name__}.{f.name} ({kind} "
                                f"{base.__name__}): bare {raw!r} -> {got!r}, "
                                f"expected exactly one element"
                            )
                # Every probe above happens to convert, so a builder that casts
                # eagerly still looks fine on a list. An unresolvable ref is the
                # case that separates them: the decorator logs and keeps it, an
                # eager cast raises out of load_config.
                if kind == "list" and base is not str:
                    args = ("probe", {f.name: ["not-a-number"]}) \
                        if klass.__name__ in two_arg else ({f.name: ["not-a-number"]},)
                    try:
                        got = getattr(from_dict(*args), f.name)
                    except Exception as e:  # noqa: BLE001 — that IS the failure
                        broken.append(
                            f"{klass.__name__}.{f.name} ({kind} {base.__name__}): "
                            f"unconvertible entry raised {type(e).__name__}: {e}"
                        )
                    else:
                        if got != ["not-a-number"]:
                            broken.append(
                                f"{klass.__name__}.{f.name} ({kind} "
                                f"{base.__name__}): unconvertible entry became "
                                f"{got!r} instead of being kept verbatim"
                            )
                # A bare `key:` in YAML must not be read as "use the default".
                if kind == "scalar" and base is bool:
                    args = ("probe", {f.name: None}) if klass.__name__ in two_arg \
                        else ({f.name: None},)
                    got = getattr(from_dict(*args), f.name)
                    if got is not False:
                        broken.append(f"{klass.__name__}.{f.name}: null -> {got!r}")
        assert checked > 60, f"probe only reached {checked} fields — did it break?"
        assert not broken, "fields that ignore their declared type:\n" + "\n".join(broken)

    def test_every_declared_list_field_wraps_a_bare_scalar(self):
        """Walk the declared annotations, not the coercion's own idea of them.

        The sweep above asks `_classify` which fields it handles, so a type the
        coercion silently ignores is invisible to it — a field it skips is a
        field that test never probes. That is exactly how `list[str]` stayed
        broken while the sweep stayed green. This one derives its field set from
        `typing.get_type_hints`, so a `list[X]` the coercion does not cover fails
        here instead of disappearing.

        The rule: a `${VAR}` reference on a list field interpolates to a bare
        string and must arrive as exactly one element. Both ways of getting that
        wrong — widening it (`list("a@b.com")` is nineteen entries) and splitting
        it (`redact_patterns` defaults contain `{20,}`) — produce a list nothing
        downstream can distinguish from a genuine one.
        """
        import dataclasses
        import inspect
        import typing

        import nerve.config as cfg

        bare = {
            str: ("alpha,beta", ["alpha,beta"]),
            int: ("8900", [8900]),
            float: ("1.5", [1.5]),
            bool: ("true", [True]),
        }
        two_arg = {"McpServerConfig"}  # from_dict(cls, name, d)
        broken, checked, skipped = [], 0, []
        for _name, klass in sorted(vars(cfg).items()):
            if not (inspect.isclass(klass) and dataclasses.is_dataclass(klass)):
                continue
            from_dict = getattr(klass, "from_dict", None)
            if not callable(from_dict):
                continue
            hints = typing.get_type_hints(klass)
            for f in dataclasses.fields(klass):
                declared = hints.get(f.name)
                if typing.get_origin(declared) is not list:
                    continue
                args = typing.get_args(declared)
                element = args[0] if args else None
                if element not in bare:
                    skipped.append((f"{klass.__name__}.{f.name}", element))
                    continue
                raw, expected = bare[element]
                checked += 1
                call = ("probe", {f.name: raw}) if klass.__name__ in two_arg \
                    else ({f.name: raw},)
                got = getattr(from_dict(*call), f.name)
                if got != expected:
                    broken.append(
                        f"{klass.__name__}.{f.name} (list[{element.__name__}]): "
                        f"bare {raw!r} -> {got!r}, expected {expected!r}"
                    )
        assert checked >= 19, f"probe reached only {checked} list fields — did it break?"
        assert not broken, (
            "list fields that do not wrap a bare scalar as a single element:\n"
            + "\n".join(broken)
        )
        # The skip bucket may only ever hold lists of dataclasses, where a bare
        # string is not shorthand for a mapping and the builder fails loudly on
        # its own. A new `list[<scalar>]` must not land here unprobed.
        for name, element in skipped:
            assert dataclasses.is_dataclass(element), (
                f"{name} is list[{element}], which has no probe above — add one "
                "rather than leaving the field unchecked"
            )

    def test_defaults_arrive_as_lists_not_tuples(self):
        """`from_dict({})` must hand every `list[X]` field an actual list.

        The sweeps above feed values in; none of them ever checked what falls
        out when a section is absent and the builder supplies its own default.
        A default spelled as a module-level tuple (immutable, safe to share)
        that a builder passes through unconverted reaches `coerce_scalars` as
        a non-list — one "ignoring non-list config value" warning per config
        load, on configs that never mention the section at all. That is how
        `langfuse.redact_patterns` shipped. Tuples now normalize like any
        other code-side scalar, and this sweep pins the container type of
        every built default so the next tuple constant fails here instead of
        warning in production.
        """
        import dataclasses
        import inspect
        import typing

        import nerve.config as cfg

        two_arg = {"McpServerConfig"}  # from_dict(cls, name, d)
        broken, checked = [], 0
        for _name, klass in sorted(vars(cfg).items()):
            if not (inspect.isclass(klass) and dataclasses.is_dataclass(klass)):
                continue
            from_dict = getattr(klass, "from_dict", None)
            if not callable(from_dict):
                continue
            hints = typing.get_type_hints(klass)
            list_fields = [
                f.name
                for f in dataclasses.fields(klass)
                if typing.get_origin(hints.get(f.name)) is list
            ]
            if not list_fields:
                continue
            built = from_dict("probe", {}) if klass.__name__ in two_arg \
                else from_dict({})
            for name in list_fields:
                checked += 1
                got = getattr(built, name)
                if not isinstance(got, list):
                    broken.append(
                        f"{klass.__name__}.{name}: from_dict({{}}) built a "
                        f"{type(got).__name__}: {got!r}"
                    )
        assert checked >= 19, f"probe reached only {checked} list fields — did it break?"
        assert not broken, (
            "list fields whose built defaults are not lists:\n" + "\n".join(broken)
        )

    def test_langfuse_default_redact_patterns_load_cleanly(self, caplog):
        """An absent langfuse block keeps the default redact set, silently.

        Regression: the defaults constant is a tuple, and `from_dict` used to
        hand it through verbatim — the patterns still compiled (the consumer
        `list()`s the field), but every single config load logged a
        `nerve.coerce` warning about it.
        """
        import logging

        from nerve.config import _DEFAULT_LANGFUSE_REDACT_PATTERNS, LangfuseConfig

        with caplog.at_level(logging.WARNING, logger="nerve.coerce"):
            lf = LangfuseConfig.from_dict({})
        assert lf.redact_patterns == list(_DEFAULT_LANGFUSE_REDACT_PATTERNS)
        coerce_records = [r for r in caplog.records if r.name == "nerve.coerce"]
        assert not coerce_records, [r.getMessage() for r in coerce_records]
