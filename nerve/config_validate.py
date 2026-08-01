"""Config-bundle validation for CI and ``nerve config validate``.

Validates the merged configuration the way :func:`nerve.config.load_config`
assembles it (workspace/config/settings.yaml + config.yaml + config.local.yaml),
but **leniently** with respect to secrets: ``${ENV_VAR}`` references that are
unset are treated as valid placeholders (they aren't present in CI) rather than
hard failures — unless ``strict_env`` is requested.

Structural problems are hard errors so a bad config PR fails CI before merge: an
unparseable or invalid cron file, a bad backend / codex config, a malformed cron
gate spec, a schedule the scheduler will not run as written
(:func:`_schedule_problem`), or a path setting written ``.`` — which aims nerve
at the daemon's working directory (:data:`_WORKING_DIR_PATH_KEYS`). Unknown /
misspelled top-level keys are warnings by default (a config carrying a key from
a newer nerve, or the shipped example, shouldn't fail CI) — pass ``strict_keys``
to promote them to errors.

**Validation never loads the bundle's own code.** Cron gate plugins are ordinary
``.py`` files that the daemon imports at startup; importing one to check it would
mean the bundle had already executed by the time we decided it was unfit — "an
invalid bundle is refused" is not a guarantee you can make about code you had to
run to judge. So validation does not load them at all, and it does not try to
divine what they declare either: a gate type it doesn't recognize is reported as
unverified, not accepted and not rejected. A plugin is code, and code is checked
by running it — that is the author's job, not the config validator's.

Built-in gate specs *are* built (``build_gate``), which runs nerve's own
``from_config`` with values from the bundle. That is what type-checking a gate
spec means; those three bodies parse their arguments and have no side effects.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any

from nerve import config as cfg


@dataclass
class ValidationResult:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    info: list[str] = field(default_factory=list)
    #: Names of ``${VAR}`` references the bundle needs and the environment does
    #: not have. Kept as data rather than only as prose, because which bucket the
    #: prose lands in depends on ``strict_env`` — and a caller that tolerates
    #: unset variables still needs to know the daemon will refuse this config.
    unresolved_env: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def validate_config_bundle(
    config_dir: Path,
    workspace_override: Path | str | None = None,
    strict_env: bool = False,
    strict_keys: bool = False,
    portable_only: bool = False,
    assume_locked: bool = False,
) -> ValidationResult:
    """Validate the config bundle rooted at ``config_dir``.

    ``workspace_override`` forces the workspace location (e.g. a checked-out
    config repo in CI: ``--workspace .``) instead of resolving it from
    config.yaml. ``strict_env`` promotes unset ``${ENV_VAR}`` references from
    info to errors. ``strict_keys`` promotes unknown/misspelled keys from
    warnings to errors (off by default so a config carrying a key from a newer
    nerve, or the shipped example, doesn't spuriously fail CI).

    ``portable_only`` drops the machine-local layers and judges the portable
    workspace config alone — the right question for a change headed to a shared
    repo, and the only way to get an answer that doesn't depend on the host
    running the check. It covers cron too: the loader's fallback to a legacy
    ``~/.nerve/cron`` is suppressed, so only the bundle's own cron files count.
    Pair it with ``workspace_override``: with no machine config left to read the
    workspace location from, it falls back to the default one.

    ``assume_locked`` judges the locked view whatever the bundle's own
    ``lockdown`` flag resolves to here. A fleet repo states the flag as
    ``${NERVE_LOCKDOWN:-false}`` so that one bundle can serve locked and unlocked
    boxes — which means CI, where the variable is unset, resolves it to false and
    validates the view *no locked box will ever run*. The lockdown checks then
    have nothing to fire on and the only machine that finds out is the locked one,
    at boot. This is how a config repo whose members include a locked instance
    checks the configuration that instance will actually load.
    """
    result = ValidationResult()
    config_dir = Path(config_dir)

    # The two machine-local layers. Skipped under portable_only: an override on
    # the validating host can otherwise mask an invalid shared value, and — the
    # commoner failure — a broken local file condemns a shared bundle that has
    # nothing wrong with it, blaming an error the bundle doesn't contain.
    machine_paths = (config_dir / "config.yaml", config_dir / "config.local.yaml")
    # ``None`` marks a layer that was not read — skipped, or unusable.
    layers: list[dict[str, Any] | None] = [None, None]
    if not portable_only:
        layers = [_read_layer(p, result) for p in machine_paths]
    overlaid = [
        p.name for p, layer in zip(machine_paths, layers)
        if layer is not None and p.exists()
    ]
    base, local = layers[0] or {}, layers[1] or {}
    machine = cfg._deep_merge(base, local)

    # An environment anchor forces lockdown regardless of the files, so a run in
    # the daemon's environment has to judge the same thing the daemon will.
    # Errors from a malformed anchor are reported rather than raised — the
    # validator's job is to say what is wrong, not to become unusable.
    try:
        anchored = cfg.lockdown_anchor()
    except cfg.ConfigError as e:
        result.errors.append(str(e))
        anchored = False

    if workspace_override is not None:
        workspace = Path(workspace_override).expanduser()
    elif anchored:
        # Same rule as the loader: the anchor selects the workspace too, so an
        # unpinned run in an anchored environment looks at the tree the daemon
        # will. An explicit --workspace still wins — pointing the validator at a
        # checkout is the whole point of the flag, and CI is not the anchored box.
        try:
            workspace = cfg._anchored_workspace()
        except cfg.ConfigError as e:
            result.errors.append(str(e))
            workspace = cfg.paths.default_workspace()
    else:
        # Resolve the workspace the same way load_config does, incl. best-effort
        # ${VAR}/${VAR:-default} interpolation of the path itself. Under
        # portable_only there is no machine config to read it from, so this
        # falls back to the default location — pass workspace_override to point
        # at a checkout.
        ws_raw = machine.get("workspace")
        if isinstance(ws_raw, str) and "${" in ws_raw:
            ws_raw = cfg._interpolate_str(ws_raw, [])
        workspace = cfg._expand_path(ws_raw) or cfg.paths.default_workspace()

    ws_settings = _read_workspace_settings(workspace, result)
    # Mirror _read_config_sources: when the tracked settings lock the instance,
    # validate the LOCKED view (workspace-only), since that's what production runs.
    # An unreadable flag is refused rather than assumed, exactly as at load time —
    # reported here, before the change is merged, which is the whole point. That
    # error is already fatal, so nothing is fail-open in carrying on with the
    # unlocked view; judging the locked one instead would bury the real problem
    # under a pile of errors derived from a lockdown the bundle never asked for.
    raw_lockdown = ws_settings.get("lockdown")
    try:
        locked = cfg._as_lockdown(raw_lockdown)
    except cfg.ConfigError as e:
        result.errors.append(str(e))
        locked = False
    if anchored and not locked:
        result.info.append(
            f"validating the LOCKED view: {cfg.LOCKDOWN_ANCHOR_ENV} is set in "
            f"this environment, which locks the instance whatever the tracked "
            f"settings say"
        )
        locked = True
    if assume_locked and not locked:
        result.info.append(
            "validating the LOCKED view on request (--assume-lockdown); this "
            "bundle's own lockdown flag resolves to false in this environment"
        )
        locked = True
    elif not locked and isinstance(raw_lockdown, str) and "${" in raw_lockdown:
        # The flag is env-controlled and this environment says off, so everything
        # below judges a view no locked member of the fleet will run. Say so:
        # nobody would otherwise think to ask for the other one.
        result.warnings.append(
            f"lockdown is set from the environment ({raw_lockdown}) and resolves "
            f"to false here, so the locked view was NOT validated — a locked "
            f"instance loads a different config than this. Re-run with "
            f"--assume-lockdown to check it."
        )
    if locked:
        merged = dict(ws_settings)
    else:
        merged = cfg._deep_merge(cfg._deep_merge(ws_settings, base), local)
    merged["lockdown"] = locked
    # Judge the same view the instance will run, so a locked bundle is checked on
    # its tracked values alone. Before the pin below overwrites the bundle's own
    # ``workspace`` value with the resolved one — that value is under review too.
    _validate_working_dir_paths(merged, result)
    # Pin the workspace so cron paths resolve against the validated workspace.
    merged["workspace"] = str(workspace)

    # Lenient env interpolation — collect unset refs without raising.
    missing: list[str] = []
    merged = cfg._interpolate_env(merged, missing)
    env_names = ", ".join(sorted(set(missing)))
    result.unresolved_env = sorted(set(missing))
    if missing:
        msg = f"references unset environment variable(s): {env_names}"
        # Say which layer asked for them. The refs are collected from the merged
        # config, so a variable named only by this host's config.yaml /
        # config.local.yaml is otherwise indistinguishable from one the portable
        # bundle needs — and when this runs as sync's gate, that reads as a defect
        # in an incoming change that has nothing to do with it.
        #
        # Which is why the workspace layer is scanned separately rather than
        # inferred from the machine one: a var both layers name would otherwise be
        # reported as the machine's alone, sending the reader to a file whose only
        # fault is that it also needs the variable, while the portable config that
        # equally needs it goes unmentioned. Only claim exclusivity when the
        # workspace config genuinely does not ask for the variable.
        from_machine: list[str] = []
        cfg._interpolate_env(machine, from_machine)
        from_workspace: list[str] = []
        cfg._interpolate_env(ws_settings, from_workspace)
        unset = set(missing)
        machine_only = sorted((set(from_machine) - set(from_workspace)) & unset)
        both_layers = sorted(set(from_machine) & set(from_workspace) & unset)
        clauses = []
        if machine_only:
            clauses.append(
                f"{', '.join(machine_only)} referenced only by this machine's "
                f"config.yaml/config.local.yaml, not by the workspace config"
            )
        if both_layers:
            clauses.append(
                f"{', '.join(both_layers)} referenced by both the workspace config "
                f"and this machine's config.yaml/config.local.yaml"
            )
        if clauses:
            msg += " — " + "; ".join(clauses)
        (result.errors if strict_env else result.info).append(msg)

    # Unknown / misspelled keys: warnings by default (forward-compat / example
    # configs shouldn't fail CI), errors under --strict-keys.
    key_problems = cfg.validate_config_keys(merged)
    for w in key_problems:
        bucket = result.errors if strict_keys else result.warnings
        bucket.append(f"unknown or invalid config key: {w}")
    if key_problems and strict_keys:
        # The key set is this validator's own. Under --strict-keys that is the one
        # check whose verdict can differ between two nerve versions, so a CI run
        # that disagrees with the instance is a version difference rather than a
        # bad config. Say it where the failure is read, or a config repo ends up
        # blocked by a key its own instance understands with nothing to explain it.
        result.info.append(
            "unknown-key errors reflect the config keys this nerve version knows; "
            "validate with the version you deploy"
        )

    # Typed construction surfaces backend / codex / structural validation errors.
    # An unset ${VAR} left as a literal string can trip one of those checks on its
    # own — "${NERVE_BACKEND}" is not a known backend name — which is a
    # consequence of the missing variable, already reported, rather than a config
    # bug. But that cannot excuse the whole construction: secrets are unset in CI,
    # so "some variable is missing" is the normal state there, and downgrading
    # every error under it disables this check exactly where it runs. So the
    # failure is re-derived with the unresolved references out of the way, and only
    # an error that disappears is charged to the environment.
    config = None
    try:
        config = cfg.NerveConfig.from_dict(merged)
        config.config_dir = config_dir
    except Exception as e:  # noqa: BLE001 — ConfigError / ValueError from validate()
        residual = e
        if missing and not strict_env:
            residual = _error_without_unresolved(merged)
        if residual is None:
            result.warnings.append(
                f"could not fully type-check config while env var(s) are unset "
                f"({env_names}): {e}"
            )
        else:
            # The residual error rather than ``e``: the first failure may itself
            # be env-caused, with a real one behind it.
            result.errors.append(f"config error: {residual}")

    if locked:
        # Judged on the workspace the run was pointed at, so a candidate bundle
        # is checked against its own checkout rather than this machine's tree.
        result.errors.extend(cfg.lockdown_workspace_problems(workspace))
    if config is not None:
        # The machine overlay read at the top of this function is what a locked
        # instance discards, so pass it through: with it the note can name the
        # keys this bundle strands rather than describing the hazard in general.
        # Empty under portable_only, which the callee guards against.
        result.warnings.extend(
            cfg.lockdown_machine_local_notes(merged, machine if locked else None)
        )

    # Resolve the cron locations even if full typed construction failed above.
    if config is not None:
        cron_files = (
            ("system", config.cron.system_file),
            ("jobs", config.cron.jobs_file),
        )
    else:
        base_cron = cfg._resolve_cron_dir(workspace, locked=locked)
        cron_files = (
            ("system", base_cron / "system.yaml"),
            ("jobs", base_cron / "jobs.yaml"),
        )

    portable_root = cfg.workspace_config_dir(workspace)
    if portable_only:
        cron_files = _tracked_cron_only(cron_files, portable_root, result)

    _validate_cron(cron_files, result, strict_keys=strict_keys)
    if config is not None:
        # Source runners are scheduled from the same parser as cron jobs, so a
        # typo'd sync schedule fails identically — and needs the same gate.
        _validate_source_schedules(config, result)
    # Files of the bundle that were actually opened, so "it validated" can be
    # told from "it found nothing to validate".
    read = [
        p for p in (cfg.workspace_settings_file(workspace), *(p for _, p in cron_files))
        if p.is_file() and p.is_relative_to(portable_root)
    ]
    _note_layers(
        config_dir, workspace, result,
        portable_only=portable_only,
        workspace_pinned=workspace_override is not None,
        overlaid=overlaid,
        portable_read=read,
    )
    return result


def _tracked_cron_only(cron_files, portable_root: Path, result: ValidationResult):
    """Keep cron on the tracked bundle's own files, for ``portable_only``.

    Cron resolution is file-aware: a workspace carrying no jobs falls back to the
    machine-local ``~/.nerve/cron`` so an un-migrated install keeps working. That
    fallback is right at load time and wrong here — it hands a run that was asked
    to judge the shared bundle alone a file the bundle does not contain, and a
    typo in it fails a config repo that has nothing wrong with it. Same substitution
    the machine-local layers are skipped to prevent, one directory over.

    An explicit ``cron.jobs_file`` pointing outside the tracked subtree goes the
    same way: whatever it is, it isn't what the repo carries.
    """
    kept = []
    for label, path in cron_files:
        if path.is_relative_to(portable_root):
            kept.append((label, path))
            continue
        tracked = portable_root / "cron" / f"{label}.yaml"
        kept.append((label, tracked))
        if path.exists():
            result.info.append(
                f"cron {label} was not read from {path}: it is outside the tracked "
                f"config and this run judges the portable bundle alone"
            )
    return tuple(kept)


def _read_layer(path: Path, result: ValidationResult) -> dict[str, Any] | None:
    """Read one machine-local layer; ``None`` if it could not be used.

    These reads happen before anything is type-checked, so an unparseable layer
    would otherwise escape as a traceback — the ugliest possible output for what
    is the likeliest failure of all, a YAML typo.

    Read strictly. A layer that parses to the wrong shape — a list, from a merge
    conflict resolved into a sequence, or a truncated write — is *dropped* in
    lenient mode: every key it supplies silently reverts to the layer below,
    including the ``workspace`` path, so validation walks off to a different
    tree and reports it clean. ``load_config`` can't afford to fail there;
    the validator exists to.
    """
    try:
        return cfg._read_yaml_mapping(path, strict=True)
    except cfg.ConfigError as e:
        result.errors.append(str(e))
    except OSError as e:
        result.errors.append(f"cannot read {path}: {e}")
    return None


def _read_workspace_settings(workspace: Path, result: ValidationResult) -> dict[str, Any]:
    """Read the portable ``settings.yaml`` layer, reporting a bad file."""
    try:
        return cfg._load_workspace_settings(workspace)
    except cfg.ConfigError as e:
        result.errors.append(str(e))
    except OSError as e:
        result.errors.append(
            f"cannot read {cfg.workspace_settings_file(workspace)}: {e}"
        )
    return {}


def _error_without_unresolved(merged: dict[str, Any]) -> Exception | None:
    """The construction error that is not an artifact of unset ``${VAR}`` refs.

    Rebuilds the config with every value that still carries a literal ``${...}``
    dropped, so those keys fall back to their defaults, and returns whatever it
    still raises — ``None`` if it now builds, which means the missing variables
    and nothing else caused the original failure.

    Dropping is what makes the question decidable without inventing values: a
    default is a value nerve ships, so it cannot be the thing a check rejects,
    and an error that survives is therefore stated in the bundle. The other
    direction is deliberately out of scope: whether the value a *set* variable
    would carry is valid cannot be known from the bundle, which is what
    ``strict_env`` and the daemon's own load are for.
    """
    try:
        cfg.NerveConfig.from_dict(_without_unresolved(merged))
    except Exception as e:  # noqa: BLE001 — same surface as the caller's
        return e
    return None


def _without_unresolved(value: Any) -> Any:
    """*value* with every setting whose text still holds a ``${...}`` removed."""
    if isinstance(value, dict):
        return {
            k: _without_unresolved(v)
            for k, v in value.items() if not _is_unresolved(v)
        }
    if isinstance(value, list):
        return [_without_unresolved(v) for v in value if not _is_unresolved(v)]
    return value


def _is_unresolved(value: Any) -> bool:
    """True for a value whose own text still carries a ``${...}`` reference.

    Only reached on the interpolated config, where every reference that *could*
    be resolved already was — so what is left is unset by definition.
    """
    return isinstance(value, str) and "${" in value


#: Path settings that must not resolve to the process's working directory, paired
#: with what aiming each one there would actually do.
#:
#: The value that gets here is an explicit ``.``, ``./`` or ``./.``: those survive
#: :func:`nerve.config._expand_path` as the path they are, so the setting really
#: does mean "wherever the daemon was started". A *blank* value is not one of
#: these — ``_expand_path`` maps it to ``None`` and the field takes its documented
#: default, which is why nothing below applies to it.
#:
#: Nothing legitimate wants any of these aimed at a working directory, and a
#: bundle whose meaning depends on where the daemon was launched from is what a
#: config gate exists to stop, so each is a hard error regardless of strictness.
#: Kept in step with the config dataclasses by the coverage test in
#: tests/test_config_validate.py: a new ``Path`` setting has to be listed here or
#: exempted there.
_WORKING_DIR_PATH_KEYS: dict[tuple[str, ...], str] = {
    ("workspace",): (
        "the entire workspace (settings.yaml, the cron config, memory, "
        "everything nerve syncs) would be read from and written to whatever "
        "directory the daemon was started in"
    ),
    ("cron", "gate_plugins_dir"): (
        "every *.py file in that directory is imported and executed by the "
        "daemon, at startup and again on every cron reload, so this hands the "
        "working directory's contents to nerve as code"
    ),
    ("cron", "jobs_file"): "cron would try to read a directory as its jobs file",
    ("cron", "system_file"): (
        "cron would try to read a directory as its system-jobs file"
    ),
    ("gateway", "ssl", "cert"): (
        "TLS is 'on' precisely when cert and key are both set, so this turns it "
        "on with a directory as the certificate file and the gateway fails to "
        "bind at startup"
    ),
    ("gateway", "ssl", "key"): (
        "TLS is 'on' precisely when cert and key are both set, so this turns it "
        "on with a directory as the private-key file and the gateway fails to "
        "bind at startup"
    ),
    ("proxy", "binary_path"): (
        "a directory satisfies the exists-and-executable check that decides "
        "whether to download the proxy, so nerve skips the download and tries "
        "to exec the working directory"
    ),
    ("proxy", "auth_dir"): (
        "the proxy's credential store (its OAuth tokens) would be created in "
        "whatever directory the daemon was started in"
    ),
    ("proxy", "log_file"): (
        "the proxy's output is opened for append at that path, which a directory "
        "refuses, so the proxy does not start"
    ),
    ("workflows", "runs_dir"): (
        "every workflow run's journal directory would be created in whatever "
        "directory the daemon was started in, and the API's containment check "
        "on run paths would be rooted there too"
    ),
}


def _validate_working_dir_paths(
    merged: dict[str, Any], result: ValidationResult,
) -> None:
    """Refuse path settings that resolve to the process's working directory.

    Checked on the raw bundle values rather than on the constructed config: by
    the time these have become ``Path`` objects an explicit ``.`` is
    indistinguishable from a deliberate choice, and the point is to name the
    text the author wrote back to them.
    """
    for key, consequence in _WORKING_DIR_PATH_KEYS.items():
        raw = _nested_get(merged, key)
        if not isinstance(raw, str):
            continue
        # ``${VAR:-.}`` reaches the same directory, one indirection later.
        # Expand best-effort as elsewhere here; an unset *required* ``${VAR}``
        # stays literal (so it isn't flagged) and is reported on its own.
        value = cfg._interpolate_str(raw, []) if "${" in raw else raw
        reason = _working_dir_reason(value)
        if reason is None:
            continue
        shown = repr(raw) if value == raw else f"{raw!r} (expands to {value!r})"
        result.errors.append(
            f"{'.'.join(key)}: {shown} {reason} — {consequence}. Remove the key "
            f"to use the default, or set an explicit path."
        )


def _working_dir_reason(value: str) -> str | None:
    """Why *value* points at the process's working directory, or ``None``.

    Asked of ``_expand_path``, the function the daemon resolves these settings
    with, so the two cannot disagree about which values reach a working
    directory and which are the blank that means "use the default".
    """
    if cfg._expand_path(value) == Path("."):  # "." , "./" , "./."
        return "resolves to the process's working directory"
    return None


def _nested_get(d: dict[str, Any], key: tuple[str, ...]) -> Any:
    """Follow a dotted key into nested mappings; ``None`` if the path breaks."""
    for part in key[:-1]:
        d = d.get(part)
        if not isinstance(d, dict):
            return None
    return d.get(key[-1])


def _note_layers(
    config_dir: Path,
    workspace: Path,
    result: ValidationResult,
    *,
    portable_only: bool,
    workspace_pinned: bool,
    overlaid: list[str],
    portable_read: list[Path],
) -> None:
    """Name every layer the verdict was reached on — always, including none.

    A report that lists nothing is ambiguous in three directions: an error may
    have come from a file that only exists on the machine running the check, a
    clean result may only be clean because a local override papered over it,
    and — the one that makes a CI gate worthless — a run may have looked at a
    workspace that isn't the tree under review, and passed because there was
    nothing there.

    Paths are absolute on purpose: ``--workspace .`` would otherwise report
    ``config/settings.yaml``, which answers none of the above.
    """
    config_dir = config_dir.resolve()
    settings = cfg.workspace_settings_file(workspace).resolve()
    found = "" if settings.is_file() else " (not present)"
    result.info.append(f"portable layer: {settings}{found}")

    if portable_only:
        result.info.append(
            "machine-local layers (config.yaml, config.local.yaml) not read: "
            "validating the portable workspace config on its own"
        )
        if not workspace_pinned:
            # Dropping the machine layers also drops the workspace path they
            # carried, so an unpinned run silently moves to the default tree.
            result.warnings.append(
                f"no workspace was given and machine-local config was not read, "
                f"so the workspace fell back to the default ({workspace}) — pass "
                f"a workspace to validate a specific checkout"
            )
        if not portable_read:
            # Asked to judge the portable layer, and not one of its files was
            # opened. The usual cause is a layout mistake — settings.yaml at the
            # repo root, a .yml extension, a config/ holding only placeholders —
            # and passing here would be a gate that checked nothing, for good.
            result.errors.append(
                f"nothing to validate: no config files were found under "
                f"{cfg.workspace_config_dir(workspace).resolve()} (looked for "
                f"settings.yaml, cron/system.yaml, cron/jobs.yaml)"
            )
        return

    present = [
        p.name for p in (
            config_dir / "config.yaml", config_dir / "config.local.yaml",
        ) if p.exists()
    ]
    if not present:
        result.info.append(f"no machine-local layers present in {config_dir}")
    elif not overlaid:
        # Present but unusable — the errors say why; don't claim they applied.
        result.info.append(
            f"machine-local layer(s) could not be used: "
            f"{', '.join(present)} ({config_dir})"
        )
    else:
        note = (
            f"machine-local layer(s) overlaid on the portable config: "
            f"{', '.join(overlaid)} ({config_dir})"
        )
        if result.errors:
            note += (
                " — an error in this run may originate there rather than in "
                "the workspace config"
            )
        result.info.append(note)


def _validate_cron(
    cron_files, result: ValidationResult, *, strict_keys: bool = False,
) -> None:
    """Validate cron files strictly, including gate specs (which build_gates
    otherwise swallows)."""
    from nerve.cron.gates import GATE_REGISTRY, GateConfigError, build_gate
    from nerve.cron.jobs import load_jobs

    # One note per distinct unverifiable gate type, however many jobs use it.
    reported: set[str] = set()

    for label, path in cron_files:
        # Collect per-job failures rather than raising on the first: a job whose
        # gate spec won't build is refused at run time (see build_gates), and
        # stopping here would hide every other problem in the same file.
        job_errors: list[str] = []
        try:
            jobs = load_jobs(
                path, strict=True, errors=job_errors, build_gates=False,
            )
        except cfg.ConfigError as e:
            result.errors.append(str(e))
            continue
        except Exception as e:  # noqa: BLE001 — job construction may raise oddly
            result.errors.append(f"cron {label} ({path}): {e}")
            continue
        result.errors.extend(job_errors)
        if path.exists():
            result.info.append(f"cron {label}: {len(jobs)} job(s) ({path})")
        # The jobs that built are still worth checking spec by spec: load_jobs
        # only reports the ones it could not construct at all, and a spec can be
        # buildable yet wrong — an unknown field takes the default silently.
        for job in jobs:
            where = f"cron {label} job '{job.id}'"
            problem = _schedule_problem(job.schedule)
            if problem:
                result.errors.append(f"{where}: {problem}")
            for spec in _job_gate_specs(job, where, result):
                problem = _gate_spec_problem(spec)
                if problem:
                    result.errors.append(f"{where}: invalid gate {spec!r}: {problem}")
                    continue
                gate_type = spec["type"]
                cls = GATE_REGISTRY.get(gate_type)
                if cls is None:
                    if gate_type in reported:
                        continue
                    # Could be a gate plugin's type, could be a typo — telling
                    # them apart means loading the plugin, i.e. running the
                    # bundle. Say which it can't confirm instead of guessing,
                    # and say what happens if nothing provides it.
                    reported.add(gate_type)
                    result.warnings.append(
                        f"{where}: gate type {gate_type!r} is not a built-in "
                        f"gate. A gate plugin may provide it; validation does "
                        f"not load plugins, so it can confirm neither the type "
                        f"nor this spec's fields. If nothing registers it at "
                        f"run time the job does not load at all — a reload is "
                        f"refused and startup skips the job"
                    )
                    continue
                try:
                    build_gate(spec)
                except GateConfigError as e:
                    result.errors.append(f"{where}: invalid gate {spec}: {e}")
                    continue
                # A misspelled field is the quiet failure: from_config reads the
                # spec with .get(), so the typo takes the default and the gate
                # silently checks something else.
                unknown = sorted(set(spec) - {"type"} - cls.spec_keys)
                if unknown and cls.spec_keys:
                    bucket = result.errors if strict_keys else result.warnings
                    bucket.append(
                        f"{where}: gate {gate_type!r} ignores unknown field(s) "
                        f"{', '.join(unknown)} (known: "
                        f"{', '.join(sorted(cls.spec_keys))})"
                    )


def _schedule_problem(schedule: Any) -> str | None:
    """Why the daemon would not run *schedule* as its author wrote it.

    Asked of the scheduler's own parser rather than re-derived here: a
    validator that disagrees with the daemon is worse than no validator,
    because it either fails configs that work or passes configs that don't.

    Two distinct mistakes, both silent without this check:

    * a 5-field crontab the scheduler rejects (``"99 * * * *"``) — the daemon
      does refuse to schedule that job, but only once the change has merged
      and synced, and only into a log line, with the instance left on its old
      config until someone reads it;
    * a string that is neither a crontab nor an interval (``"hourly"``,
      ``"@daily"``) — nothing complains about this one *ever*: it falls back
      to a fixed default and the job runs happily on a cadence nobody chose.

    Both are hard errors. Unlike an unknown config key, there is no
    forward-compatibility case to protect: the accepted schedule forms are
    read out of the scheduler code this validator is importing, so if nerve
    learns a new one the answer here changes with it. And nothing the daemon
    would honour is flagged — every schedule that yields the cadence its
    author asked for either parses as a crontab or carries an h/m/s token.
    """
    from nerve.cron.service import (
        InvalidScheduleError,
        NotCrontabError,
        _crontab_to_trigger,
        _interval_seconds,
        _parse_interval,
    )

    if not isinstance(schedule, str):
        # YAML hands over whatever was written: `schedule: 4` is an int, and
        # the scheduler's first move is `.split()`. That AttributeError escapes
        # every handler the daemon has for a bad schedule and takes the whole
        # cron service down with it, so it cannot reach one.
        return (
            f"schedule must be a string, got {type(schedule).__name__} "
            f"({schedule!r}) — quote it"
        )
    if "${" in schedule:
        # An unresolved ${VAR} is reported on its own; judging the literal text
        # would report a second, invented error for the same cause.
        return None
    try:
        _crontab_to_trigger(schedule)
    except NotCrontabError:
        pass  # not a crontab — must then be an interval
    except InvalidScheduleError as e:
        return str(e)
    else:
        return None

    if _interval_seconds(schedule) is None:
        return (
            f"schedule {schedule!r} is neither a 5-field crontab expression "
            f"nor an interval like '4h', '30m' or '1h30m'. Nothing rejects it "
            f"at run time — it falls back to a fixed "
            f"{_parse_interval(schedule)}s, so it runs on a cadence nobody chose"
        )
    return None


def _validate_source_schedules(config, result: ValidationResult) -> None:
    """Check ``sync.<source>.schedule`` the same way as a cron job's.

    The service turns these into triggers through the same call, so they fail
    the same way — and worse: a source that never syncs looks exactly like a
    source with nothing new, so the wrong cadence here is invisible from the
    outside for as long as it lasts.
    """
    for f in fields(config.sync):
        source = getattr(config.sync, f.name)
        if not is_dataclass(source):
            continue
        schedule = getattr(source, "schedule", None)
        if schedule is None:  # e.g. codex sync, which has no schedule
            continue
        problem = _schedule_problem(schedule)
        if problem:
            result.errors.append(f"sync.{f.name}.schedule: {problem}")


def _job_gate_specs(job, where: str, result: ValidationResult) -> list:
    """Every gate spec *job* will build, mirroring ``CronJob._build_gates``.

    The legacy ``skip_when_idle`` shorthand is turned into a ``messages`` gate
    at load time, so it needs the same checking as ``run_if`` — and one shape
    more. It takes a *list* of source names; a bare string is iterated into one
    source per character, none of which matches anything, so the gate is never
    satisfied and the job silently never runs again.
    """
    specs: list = []
    if isinstance(job.run_if, list):
        specs.extend(job.run_if)
    else:
        result.errors.append(
            f"{where}: 'run_if' must be a list of gate specs, got "
            f"{type(job.run_if).__name__}"
        )

    idle = job.skip_when_idle
    # "Not set" is an empty *list* (or a bare key, which the loader turns into
    # one) — not merely anything falsy. A falsy wrong shape such as
    # ``skip_when_idle: {}`` is exactly the case worth reporting: it reads as a
    # gate the author meant to have, and nothing can build one from it — the
    # daemon refuses the job over it.
    if not isinstance(idle, list) or not all(isinstance(s, str) for s in idle):
        shown = repr(idle) if isinstance(idle, list) else type(idle).__name__
        result.errors.append(
            f"{where}: 'skip_when_idle' must be a list of source names, got {shown}"
        )
        return specs
    if not idle:
        return specs
    specs.append({
        "type": "messages",
        "sources": list(idle),
        "consumer": job.idle_consumer,
    })
    return specs


def _gate_spec_problem(spec) -> str | None:
    """What is structurally wrong with one gate spec, if anything.

    Checked for every gate, built-in or not: the shape of a spec is config, and
    getting it wrong is the common authoring mistake — no knowledge of the gate
    itself is needed to catch it.
    """
    if not isinstance(spec, dict):
        return f"gate spec must be a mapping, got {type(spec).__name__}"
    gate_type = spec.get("type")
    if gate_type is None or gate_type == "":
        return "gate spec missing required 'type' key"
    if not isinstance(gate_type, str):
        return f"gate 'type' must be a string, got {type(gate_type).__name__}"
    if not gate_type.strip():
        return "gate spec 'type' is blank"
    return None


# -- Standalone entry point ------------------------------------------------
#
# ``python -m nerve.config_validate`` runs validation straight from a source
# checkout. Everything above needs only the standard library plus PyYAML, so a
# bundle can be checked by cloning nerve and setting PYTHONPATH — no install, no
# dependency resolution of nerve's full runtime (boto3, telethon, ...). The
# generated CI workflow installs nerve and runs ``nerve config validate``; this
# path is for environments that cannot install it.
#
# Both are the same code, and they have to stay in step in both directions:
# identical output, and the same set of flags — a flag the CLI has and this parser
# doesn't is a documented invocation this path cannot run.
# tests/test_config_validate.py asserts both.


def render_result(result: ValidationResult) -> tuple[list[str], str]:
    """Render ``result`` as (message lines, summary line).

    Shared by this module's ``main`` and the ``nerve config validate`` CLI so
    the installed and zero-install paths report identically.
    """
    lines = [f"[info] {m}" for m in result.info]
    lines += [f"[WARN] {m}" for m in result.warnings]
    lines += [f"[ERR] {m}" for m in result.errors]
    summary = (
        "Config OK" if result.ok else f"Config invalid: {len(result.errors)} error(s)"
    )
    return lines, summary


def _build_parser():
    """The standalone entry point's argument parser.

    Separate from ``main`` so a test can compare its flags with the Click
    command's without going through ``--help``.
    """
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m nerve.config_validate",
        description="Validate a Nerve config bundle. Non-zero exit on any error.",
    )
    parser.add_argument(
        "--workspace",
        default=None,
        help="Workspace to validate (a checked-out config repo: --workspace .). "
             "Defaults to the resolved workspace.",
    )
    parser.add_argument(
        "--config-dir",
        default=None,
        help="Machine-local config dir supplying config.yaml/config.local.yaml. "
             "Defaults to the resolved config dir; usually absent in CI.",
    )
    parser.add_argument(
        "--strict-env", action="store_true",
        help="Fail if any ${ENV_VAR} reference is unset (default: report as info).",
    )
    parser.add_argument(
        "--strict-keys", action="store_true",
        help="Fail on unknown/misspelled config keys (default: warn).",
    )
    parser.add_argument(
        "--portable-only", action="store_true",
        help="Ignore this machine's config.yaml/config.local.yaml and judge only "
             "the portable workspace config — what a shared repo carries. Fails "
             "if no file under the workspace's config/ was opened.",
    )
    parser.add_argument(
        "--assume-lockdown", dest="assume_locked", action="store_true",
        help="Validate the locked view whatever this bundle's lockdown flag "
             "resolves to here. A fleet repo writes `lockdown: "
             "${NERVE_LOCKDOWN:-false}`, so CI resolves it to false and checks a "
             "config no locked box will run.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Validate a config bundle and print the result. Returns the exit code."""
    args = _build_parser().parse_args(argv)

    config_dir, _ = cfg.resolve_config_dir(args.config_dir)
    result = validate_config_bundle(
        config_dir,
        workspace_override=args.workspace,
        strict_env=args.strict_env,
        strict_keys=args.strict_keys,
        portable_only=args.portable_only,
        assume_locked=args.assume_locked,
    )
    lines, summary = render_result(result)
    for line in lines:
        print(line)
    print(summary)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
