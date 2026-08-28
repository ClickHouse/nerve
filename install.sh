#!/usr/bin/env bash
#
# Nerve Installer
# https://github.com/ClickHouse/nerve
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/ClickHouse/nerve/main/install.sh | bash
#
# Environment variables:
#   NERVE_INSTALL_DIR      — Where to clone the repo (default: ~/nerve)
#   NERVE_BRANCH           — Git branch to install (default: main)
#   NERVE_YES              — Set to 1 to skip all confirmations
#   NERVE_NON_INTERACTIVE  — Set to 1 for unattended install: implies NERVE_YES,
#                            configures from env (see docs/setup.md), never prompts
#   NERVE_START            — Set to 1 to start the daemon after an unattended
#                            install; leave unset when a service manager owns it
#
set -euo pipefail

# --- Configuration ---
NERVE_REPO="https://github.com/ClickHouse/nerve.git"
NERVE_BRANCH="${NERVE_BRANCH:-main}"
INSTALL_DIR="${NERVE_INSTALL_DIR:-$HOME/nerve}"
# The CLI resolves this itself, so upgrade detection, init, start and the
# summary must all agree with it or they act on different configurations.
CONFIG_DIR="${NERVE_CONFIG_DIR:-$INSTALL_DIR}"
# 13, not 12: pyproject's requires-python is >=3.13 (set by memu-py==1.4.0).
# Accepting 3.12 here meant the installer would provision a Python that then
# failed at `uv pip install -e .` with an opaque dependency conflict.
MIN_PYTHON_MINOR=13
PREFERRED_PYTHON_MINOR=13
# 22.12, not Vite 7's 20.19 floor: @clickhouse/click-ui declares
# `engines.node >=22.12.0`, and web/package.json declares the same.
# Provisioning a 20.19 here would install a Node the dependency graph refuses
# under a strict (`--engine-strict`) install, which is a support claim we
# cannot back.
MIN_NODE_VERSION="22.12.0"
NON_INTERACTIVE="${NERVE_NON_INTERACTIVE:-0}"
AUTO_YES="${NERVE_YES:-0}"
[ "$NON_INTERACTIVE" = "1" ] && AUTO_YES=1
IS_UPGRADE=0

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# --- Utility Functions ---

info()    { printf "${CYAN}  [info]${NC} %s\n" "$1"; }
success() { printf "${GREEN}  [ok]${NC}   %s\n" "$1"; }
warn()    { printf "${YELLOW}  [warn]${NC} %s\n" "$1"; }
error()   { printf "${RED}  [err]${NC}  %s\n" "$1"; }
step()    { printf "\n${BOLD}${CYAN}==> %s${NC}\n" "$1"; }

confirm() {
    if [ "$AUTO_YES" = "1" ]; then return 0; fi
    printf "${BOLD}  %s [Y/n]${NC} " "$1"
    read -r response
    case "$response" in
        [nN]|[nN][oO]) return 1 ;;
        *) return 0 ;;
    esac
}

command_exists() { command -v "$1" >/dev/null 2>&1; }

# Root images (ubuntu:24.04 and friends) can install packages with no sudo at
# all, so elevation is a capability question, not a "is sudo present" one.
run_as_root() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    elif ! command_exists sudo; then
        error "Root privileges are required, but sudo is not available."
        return 1
    elif [ "$NON_INTERACTIVE" = "1" ]; then
        # -n so automation fails instead of waiting on a password prompt.
        sudo -n "$@"
    else
        sudo "$@"
    fi
}

# apt's -y answers apt, not debconf: tzdata still asks for a geographic area.
# The variable is set past the sudo boundary so sudo cannot strip it.
run_debian_command() {
    if [ "$NON_INTERACTIVE" = "1" ]; then
        run_as_root env DEBIAN_FRONTEND=noninteractive "$@"
    else
        run_as_root "$@"
    fi
}

# Compare versions: version_ge "3.13" "3.13" → true
version_ge() {
    local a="$1" b="$2"
    [ "$(printf '%s\n%s' "$a" "$b" | sort -V | head -n1)" = "$b" ]
}

get_python_version() {
    "$1" --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1
}

trap 'error "Installation failed at line $LINENO. See above for details."' ERR

# A clean message on Ctrl+C / SIGTERM beats silent death: tell the user
# what state they're in and how to continue.
on_interrupt() {
    trap - INT TERM ERR
    printf "\n"
    warn "Interrupted."
    if [ "${INSTALL_DONE:-0}" = "1" ]; then
        info "Installation itself is complete — finish setup anytime with: nerve init"
        info "(setup answers are saved; it resumes where you left off)"
    else
        info "Re-run the installer to continue — completed steps are skipped."
    fi
    exit 130
}
trap on_interrupt INT TERM
INSTALL_DONE=0

# --- OS Detection ---

detect_os() {
    OS="unknown"
    DISTRO="unknown"
    PKG_MGR="unknown"
    ARCH="$(uname -m)"
    # "Can we install packages", not "is sudo installed" — root needs neither.
    CAN_ELEVATE=0
    if [ "$(id -u)" -eq 0 ] || command_exists sudo; then
        CAN_ELEVATE=1
    fi

    case "$(uname -s)" in
        Linux)
            OS="linux"
            if [ -f /etc/os-release ]; then
                # shellcheck source=/dev/null
                . /etc/os-release
                case "${ID:-}" in
                    ubuntu|debian|pop|linuxmint|raspbian)
                        DISTRO="debian"; PKG_MGR="apt" ;;
                    fedora)
                        DISTRO="fedora"; PKG_MGR="dnf" ;;
                    centos|rhel|rocky|alma)
                        DISTRO="rhel"; PKG_MGR="dnf" ;;
                    arch|manjaro|endeavouros)
                        DISTRO="arch"; PKG_MGR="pacman" ;;
                    opensuse*)
                        DISTRO="suse"; PKG_MGR="zypper" ;;
                    *)
                        DISTRO="${ID:-unknown}" ;;
                esac
            fi
            ;;
        Darwin)
            OS="macos"
            DISTRO="macos"
            if command_exists brew; then
                PKG_MGR="brew"
            else
                PKG_MGR="none"
            fi
            ;;
        *)
            error "Unsupported operating system: $(uname -s)"
            exit 1
            ;;
    esac
}

# --- Dependency: git ---

ensure_git() {
    if command_exists git; then
        success "git $(git --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
        return
    fi

    info "git is not installed"
    if [ "$CAN_ELEVATE" = "0" ] && [ "$OS" = "linux" ]; then
        error "git is required but packages cannot be installed (not root, no sudo). Install git manually and re-run."
        exit 1
    fi

    if ! confirm "Install git?"; then
        error "git is required. Aborting."
        exit 1
    fi

    case "$PKG_MGR" in
        apt)
            run_debian_command apt-get update -qq && run_debian_command apt-get install -y -qq git ;;
        dnf)
            run_as_root dnf install -y -q git ;;
        pacman)
            run_as_root pacman -S --noconfirm git ;;
        zypper)
            run_as_root zypper install -y git ;;
        brew)
            brew install git ;;
        *)
            error "Don't know how to install git on $DISTRO. Install manually and re-run."
            exit 1
            ;;
    esac

    success "git installed"
}

# --- Dependency: uv ---

ensure_uv() {
    if command_exists uv; then
        success "uv $(uv --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
        return
    fi

    info "Installing uv (Python package manager)..."
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # uv installer puts it in ~/.local/bin or ~/.cargo/bin
    export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

    if ! command_exists uv; then
        error "uv installation failed. Install manually: https://docs.astral.sh/uv/"
        exit 1
    fi

    success "uv $(uv --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
}

# --- Dependency: Python 3.13+ ---

ensure_python() {
    # Check for an existing Python >= 3.$MIN_PYTHON_MINOR
    for candidate in python3.13 python3; do
        if command_exists "$candidate"; then
            local ver
            ver="$(get_python_version "$candidate")"
            if version_ge "$ver" "3.$MIN_PYTHON_MINOR"; then
                success "Python $ver ($candidate)"
                PYTHON_CMD="$candidate"
                return
            fi
        fi
    done

    # Use uv to install Python (no root required)
    info "No suitable Python found. Installing Python 3.$PREFERRED_PYTHON_MINOR via uv..."
    if uv python install "3.$PREFERRED_PYTHON_MINOR" 2>/dev/null; then
        PYTHON_CMD="$(uv python find "3.$PREFERRED_PYTHON_MINOR" 2>/dev/null || echo "")"
        if [ -n "$PYTHON_CMD" ]; then
            success "Python 3.$PREFERRED_PYTHON_MINOR installed via uv"
            return
        fi
    fi

    # Fallback: retry the minimum supported minor
    if uv python install "3.$MIN_PYTHON_MINOR" 2>/dev/null; then
        PYTHON_CMD="$(uv python find "3.$MIN_PYTHON_MINOR" 2>/dev/null || echo "")"
        if [ -n "$PYTHON_CMD" ]; then
            success "Python 3.$MIN_PYTHON_MINOR installed via uv"
            return
        fi
    fi

    # Last resort: system packages
    warn "uv python install failed. Trying system packages..."

    if [ "$CAN_ELEVATE" = "0" ] && [ "$OS" = "linux" ]; then
        error "Cannot install Python: no way to elevate and uv python install failed."
        error "Install Python 3.13+ manually and re-run."
        exit 1
    fi

    case "$PKG_MGR" in
        apt)
            if ! apt-cache show python3.13 >/dev/null 2>&1; then
                info "Adding deadsnakes PPA..."
                run_debian_command apt-get update -qq
                run_debian_command apt-get install -y -qq software-properties-common
                run_debian_command add-apt-repository -y ppa:deadsnakes/ppa
                run_debian_command apt-get update -qq
            fi
            run_debian_command apt-get install -y -qq python3.13 python3.13-venv python3.13-dev
            PYTHON_CMD="python3.13"
            ;;
        dnf)
            run_as_root dnf install -y -q python3.13 || run_as_root dnf install -y -q python3.12 || run_as_root dnf install -y -q python3
            PYTHON_CMD="$(command -v python3.13 || command -v python3.12 || command -v python3)"
            ;;
        pacman)
            run_as_root pacman -S --noconfirm python
            PYTHON_CMD="python3"
            ;;
        zypper)
            run_as_root zypper install -y python313 || run_as_root zypper install -y python312 || run_as_root zypper install -y python3
            PYTHON_CMD="$(command -v python3.13 || command -v python3.12 || command -v python3)"
            ;;
        brew)
            brew install python@3.13
            PYTHON_CMD="python3.13"
            ;;
        *)
            error "Don't know how to install Python on $DISTRO."
            error "Install Python 3.13+ manually and re-run."
            exit 1
            ;;
    esac

    if [ -z "${PYTHON_CMD:-}" ] || ! command_exists "$PYTHON_CMD"; then
        error "Failed to install Python. Install Python 3.13+ manually and re-run."
        exit 1
    fi

    # Existence alone isn't enough. The fallback chains above can settle on an
    # older interpreter (dnf/zypper try python3.13, then python3.12, then plain
    # python3), and an interpreter below the floor fails much later at
    # `uv pip install -e .` with an opaque transitive dependency conflict.
    # Fail here, where the cause is obvious, instead.
    installed_py_ver="$(get_python_version "$PYTHON_CMD")"
    if ! version_ge "$installed_py_ver" "3.$MIN_PYTHON_MINOR"; then
        error "Installed Python $installed_py_ver is below the required 3.$MIN_PYTHON_MINOR."
        error "Install Python 3.$MIN_PYTHON_MINOR+ manually and re-run."
        exit 1
    fi

    success "Python $installed_py_ver installed via system packages"
}

# --- Dependency: Node.js (floor in MIN_NODE_VERSION) ---

ensure_node() {
    if command_exists node; then
        local ver
        ver="$(node --version | tr -d 'v')"
        if version_ge "$ver" "$MIN_NODE_VERSION"; then
            success "Node.js v$ver"
            return
        fi
        warn "Node.js v$ver is too old (need v${MIN_NODE_VERSION}+)"
    fi

    info "Node.js v${MIN_NODE_VERSION}+ is not installed"

    if [ "$CAN_ELEVATE" = "0" ] && [ "$OS" = "linux" ]; then
        error "Node.js is required but packages cannot be installed (not root, no sudo)."
        error "Install Node.js v${MIN_NODE_VERSION}+ manually and re-run."
        exit 1
    fi

    if ! confirm "Install Node.js?"; then
        error "Node.js is required for the web UI. Aborting."
        exit 1
    fi

    case "$PKG_MGR" in
        apt)
            info "Installing Node.js via nodesource..."
            curl -fsSL https://deb.nodesource.com/setup_lts.x | run_debian_command bash -
            run_debian_command apt-get install -y -qq nodejs
            ;;
        dnf)
            info "Installing Node.js via nodesource..."
            curl -fsSL https://rpm.nodesource.com/setup_lts.x | run_as_root bash -
            run_as_root dnf install -y -q nodejs
            ;;
        pacman)
            run_as_root pacman -S --noconfirm nodejs npm
            ;;
        zypper)
            # nodejs22, not nodejs20: openSUSE's nodejs20 tops out below the
            # MIN_NODE_VERSION floor, so it would install and then fail the
            # check below.
            run_as_root zypper install -y nodejs22
            ;;
        brew)
            brew install node
            ;;
        none)
            if [ "$OS" = "macos" ]; then
                error "Homebrew is not installed. Install Node.js manually:"
                error "  https://nodejs.org/en/download/"
                error "Or install Homebrew first: https://brew.sh"
                exit 1
            fi
            error "Don't know how to install Node.js on $DISTRO."
            exit 1
            ;;
        *)
            error "Don't know how to install Node.js on $DISTRO."
            error "Install Node.js v${MIN_NODE_VERSION}+ manually and re-run."
            exit 1
            ;;
    esac

    if ! command_exists node; then
        error "Node.js installation failed. Install manually and re-run."
        exit 1
    fi

    # A distro's "node" package is whatever that distro froze, and nodesource's
    # setup_lts.x follows whatever upstream calls LTS today. Neither is promised
    # to clear our floor, and a too-old Node here surfaces much later as an
    # opaque syntax error inside a dependency. Check what actually landed.
    local new_ver
    new_ver="$(node --version | tr -d 'v')"
    if ! version_ge "$new_ver" "$MIN_NODE_VERSION"; then
        error "Installed Node.js v${new_ver}, but v${MIN_NODE_VERSION}+ is required."
        error "The package for $DISTRO is behind. Install a newer Node.js and re-run:"
        error "  https://nodejs.org/en/download/"
        exit 1
    fi

    success "Node.js v${new_ver} installed"
}

# --- Clone or Update Repository ---

setup_repo() {
    step "Setting up Nerve repository"

    if [ -d "$INSTALL_DIR/.git" ]; then
        info "Existing installation found at $INSTALL_DIR"
        info "Pulling latest changes..."
        git -C "$INSTALL_DIR" fetch origin "$NERVE_BRANCH" --depth 1
        if ! git -C "$INSTALL_DIR" diff --quiet 2>/dev/null; then
            warn "Local changes detected — stashing before update"
            git -C "$INSTALL_DIR" stash push -m "nerve-installer-$(date +%Y%m%d-%H%M%S)"
            info "Recover with: git -C $INSTALL_DIR stash pop"
        fi
        git -C "$INSTALL_DIR" checkout "$NERVE_BRANCH" 2>/dev/null || git -C "$INSTALL_DIR" checkout -b "$NERVE_BRANCH" "origin/$NERVE_BRANCH"
        git -C "$INSTALL_DIR" reset --hard "origin/$NERVE_BRANCH"
        IS_UPGRADE=1
        success "Repository updated"
    else
        if [ -d "$INSTALL_DIR" ] && [ "$(ls -A "$INSTALL_DIR" 2>/dev/null)" ]; then
            error "$INSTALL_DIR exists and is not empty."
            error "Set NERVE_INSTALL_DIR to a different path or remove the directory."
            exit 1
        fi
        info "Cloning Nerve to $INSTALL_DIR..."
        git clone --branch "$NERVE_BRANCH" --depth 1 "$NERVE_REPO" "$INSTALL_DIR"
        success "Repository cloned"
    fi
}

# --- Python Environment ---

setup_python_env() {
    step "Setting up Python environment"

    cd "$INSTALL_DIR" || exit 1

    # `uv sync` creates and manages .venv itself, so there's no separate venv
    # step: it installs the exact versions in uv.lock and Nerve editable.
    # Locked rather than re-resolved, so a fresh install gets the dependency set
    # that CI actually tested.
    # --locked: install the committed lock, and fail rather than silently
    #   re-resolving and rewriting uv.lock in the user's checkout.
    # --inexact: this script doubles as an upgrade path for an existing install,
    #   and `uv sync` is exact by default — without this, rerunning it would
    #   uninstall anything the user added on top (optional extras, local tools).
    info "Installing dependencies from uv.lock..."
    local sync_flags=(--locked --inexact --quiet)
    uv sync "${sync_flags[@]}" --python "3.$PREFERRED_PYTHON_MINOR" 2>/dev/null \
        || uv sync "${sync_flags[@]}" --python "3.$MIN_PYTHON_MINOR" 2>/dev/null \
        || uv sync "${sync_flags[@]}"
    success "Python environment ready"
}

# --- Build Web UI ---

build_web_ui() {
    step "Building web UI"

    cd "$INSTALL_DIR/web" || exit 1

    info "Installing npm dependencies..."
    npm ci --quiet 2>/dev/null || npm install --quiet

    info "Building React app..."
    npm run build

    success "Web UI built"
}

# --- PATH Setup ---

setup_path() {
    step "Setting up PATH"

    local nerve_binary="$INSTALL_DIR/.venv/bin/nerve"
    local local_bin="$HOME/.local/bin"
    local symlink_target="$local_bin/nerve"

    if [ ! -f "$nerve_binary" ]; then
        warn "nerve binary not found at $nerve_binary — skipping symlink"
        return
    fi

    mkdir -p "$local_bin"

    # Create or update symlink
    ln -sf "$nerve_binary" "$symlink_target"
    success "Symlinked nerve → $symlink_target"

    # Check if ~/.local/bin is already on PATH
    case ":$PATH:" in
        *":$local_bin:"*) ;;
        *)
            # Add to shell profiles
            local path_line='export PATH="$HOME/.local/bin:$PATH"'
            local added=0

            for profile in "$HOME/.bashrc" "$HOME/.zshrc" "$HOME/.profile"; do
                if [ -f "$profile" ]; then
                    if ! grep -qF '.local/bin' "$profile" 2>/dev/null; then
                        printf '\n# Added by Nerve installer\n%s\n' "$path_line" >> "$profile"
                        added=1
                    fi
                fi
            done

            if [ "$added" = "1" ]; then
                info "Added ~/.local/bin to shell profile"
            fi

            export PATH="$local_bin:$PATH"
            ;;
    esac
}

# --- Run nerve init ---

INIT_COMPLETED=0
INIT_SKIPPED=0

run_init() {
    local nerve_bin="$INSTALL_DIR/.venv/bin/nerve"
    local config_local="$CONFIG_DIR/config.local.yaml"

    if [ "$IS_UPGRADE" = "1" ] && [ -f "$config_local" ]; then
        info "Existing configuration found — skipping setup wizard"
        info "Run 'nerve init' to reconfigure"
        INIT_COMPLETED=1
        return
    fi

    if [ "$NON_INTERACTIVE" = "1" ]; then
        step "Running Nerve setup (non-interactive)"
        cd "$INSTALL_DIR" || exit 1
        # Fail loudly: a half-configured unattended install is worse than none.
        "$nerve_bin" -c "$CONFIG_DIR" init --non-interactive
        INIT_COMPLETED=1
        return
    fi

    # Clear boundary between installation and configuration: everything is
    # installed at this point — the wizard is optional and resumable.
    printf "\n"
    printf "${BOLD}${GREEN}  Installation complete.${NC}\n"
    printf "\n"
    printf "  Next: an interactive setup wizard configures your API keys,\n"
    printf "  workspace, and channels. It takes a few minutes; Ctrl+C is\n"
    printf "  safe — answers are saved and ${BOLD}nerve init${NC} resumes later.\n"
    printf "\n"

    if ! confirm "Run the setup wizard now?"; then
        info "Skipping setup. Run it later with: nerve init"
        INIT_SKIPPED=1
        return
    fi

    step "Running Nerve setup"
    cd "$INSTALL_DIR" || exit 1
    # Don't let a wizard abort look like a failed installation (set -e).
    if "$nerve_bin" -c "$CONFIG_DIR" init; then
        INIT_COMPLETED=1
    else
        printf "\n"
        warn "Setup did not finish. Installation itself is complete."
        info "Resume setup anytime with: nerve init"
        INIT_SKIPPED=1
    fi
}

# --- Offer to start the daemon ---

offer_start() {
    # Only when freshly configured and interactive
    if [ "$INIT_COMPLETED" != "1" ] || [ "$IS_UPGRADE" = "1" ]; then
        return
    fi
    # Unattended installs are usually followed by a service manager owning the
    # process, so starting a stray daemon here is wrong unless asked for.
    if [ "$NON_INTERACTIVE" = "1" ] && [ "${NERVE_START:-0}" != "1" ]; then
        info "Not starting Nerve (set NERVE_START=1 to start it here)"
        return
    fi
    printf "\n"
    if [ "$NON_INTERACTIVE" != "1" ] && ! confirm "Start Nerve now?"; then
        return
    fi
    local nerve_bin="$INSTALL_DIR/.venv/bin/nerve"
    if "$nerve_bin" -c "$CONFIG_DIR" start; then
        NERVE_STARTED=1
    else
        warn "Start failed — check logs with: nerve logs"
    fi
}

# --- Summary ---

print_summary() {
    printf "\n"
    printf "${BOLD}${GREEN}  ╔══════════════════════════════════════════╗${NC}\n"
    printf "${BOLD}${GREEN}  ║       Nerve installed successfully!      ║${NC}\n"
    printf "${BOLD}${GREEN}  ╚══════════════════════════════════════════╝${NC}\n"
    printf "\n"

    if [ "$IS_UPGRADE" = "1" ]; then
        printf "  ${BOLD}Upgrade complete.${NC} Restart to apply changes:\n"
        printf "    ${CYAN}nerve restart${NC}\n"
    elif [ "${NERVE_STARTED:-0}" = "1" ]; then
        printf "  ${BOLD}Nerve is running.${NC}\n"
        printf "    ${CYAN}http://localhost:8900${NC}     Open the web UI\n"
        printf "    ${CYAN}nerve logs${NC}            Follow daemon logs\n"
    elif [ "$INIT_SKIPPED" = "1" ]; then
        printf "  ${BOLD}Finish setup, then start:${NC}\n"
        printf "    ${CYAN}nerve init${NC}            Resume the setup wizard\n"
        printf "    ${CYAN}nerve start${NC}           Start as daemon\n"
    else
        printf "  ${BOLD}Get started:${NC}\n"
        printf "    ${CYAN}nerve start${NC}           Start as daemon\n"
        printf "    ${CYAN}nerve start -f${NC}        Start in foreground\n"
        printf "    ${CYAN}nerve doctor${NC}          Verify setup\n"
    fi

    printf "\n"
    printf "  ${BOLD}Useful commands:${NC}\n"
    printf "    ${CYAN}nerve status${NC}          Check daemon status\n"
    printf "    ${CYAN}nerve logs${NC}            Follow daemon logs\n"
    printf "    ${CYAN}nerve stop${NC}            Stop the daemon\n"
    printf "\n"
    printf "  ${DIM}Install dir : $INSTALL_DIR${NC}\n"
    printf "  ${DIM}Config      : $CONFIG_DIR/config.local.yaml${NC}\n"
    printf "  ${DIM}Data        : ~/.nerve/${NC}\n"
    printf "\n"
    printf "  ${DIM}To uninstall: rm -rf $INSTALL_DIR ~/.nerve ~/.local/bin/nerve${NC}\n"
    printf "\n"

    if ! command_exists nerve; then
        warn "nerve is not yet on PATH in this shell session"
        printf "  Run: ${BOLD}source ~/.bashrc${NC}  (or restart your terminal)\n\n"
    fi
    printf "  ${DIM}Installer finished — you can close this terminal.${NC}\n\n"
}

# --- Usage ---

usage() {
    cat <<EOF
Nerve Installer — https://github.com/ClickHouse/nerve

Usage:
  curl -fsSL https://raw.githubusercontent.com/ClickHouse/nerve/main/install.sh | bash
  curl -fsSL .../install.sh | bash -s -- --yes

Options:
  --yes, -y             Skip all confirmation prompts
  --non-interactive     Unattended install; configure from environment

Environment variables:
  NERVE_INSTALL_DIR     Where to clone (default: ~/nerve)
  NERVE_BRANCH          Git branch (default: main)
  NERVE_YES             Set to 1 to skip confirmations
  NERVE_NON_INTERACTIVE Set to 1 for unattended install (implies NERVE_YES)
  NERVE_START           Set to 1 to start the daemon after unattended install

EOF
}

# --- Main ---

main() {
    # Parse arguments
    for arg in "$@"; do
        case "$arg" in
            --yes|-y) AUTO_YES=1 ;;
            --non-interactive) NON_INTERACTIVE=1; AUTO_YES=1 ;;
            --help|-h) usage; exit 0 ;;
        esac
    done

    # When piped via curl | bash, stdin is the pipe (EOF after script).
    # Reclaim the terminal for all interactive prompts. The read test matters:
    # under cloud-init and other daemons /dev/tty exists but there is no
    # controlling terminal, so the open fails with ENXIO and set -e ends the
    # install before it starts.
    # Probe in a subshell: a failed `exec` redirection ends a non-interactive
    # shell on POSIX-mode shells, where `||` would not get a chance to run.
    if [ "$NON_INTERACTIVE" != "1" ] && [ ! -t 0 ] && (exec < /dev/tty) 2>/dev/null; then
        exec < /dev/tty
    fi

    printf "\n"
    printf "${BOLD}${CYAN}  ╔══════════════════════════════════════════╗${NC}\n"
    printf "${BOLD}${CYAN}  ║           Nerve Installer                ║${NC}\n"
    printf "${BOLD}${CYAN}  ╚══════════════════════════════════════════╝${NC}\n"
    printf "\n"
    printf "  ${DIM}Install dir : $INSTALL_DIR${NC}\n"
    printf "  ${DIM}Branch      : $NERVE_BRANCH${NC}\n"
    printf "\n"

    detect_os
    info "Detected $OS ($DISTRO) $ARCH"

    step "Checking dependencies"
    ensure_git
    ensure_uv
    ensure_python
    ensure_node

    setup_repo
    setup_python_env
    build_web_ui
    setup_path
    INSTALL_DONE=1
    run_init
    offer_start
    print_summary
    exit 0
}

main "$@"
