#!/usr/bin/env bash
#
# Nerve Installer
# https://github.com/ClickHouse/nerve
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/ClickHouse/nerve/main/install.sh | bash
#
# Environment variables:
#   NERVE_INSTALL_DIR  — Where to clone the repo (default: ~/nerve)
#   NERVE_BRANCH       — Git branch to install (default: main)
#   NERVE_YES          — Set to 1 to skip all confirmations
#
set -euo pipefail

# --- Configuration ---
NERVE_REPO="https://github.com/ClickHouse/nerve.git"
NERVE_BRANCH="${NERVE_BRANCH:-main}"
INSTALL_DIR="${NERVE_INSTALL_DIR:-$HOME/nerve}"
MIN_PYTHON_MINOR=12
PREFERRED_PYTHON_MINOR=13
MIN_NODE_MAJOR=18
AUTO_YES="${NERVE_YES:-0}"
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

# Compare versions: version_ge "3.13" "3.12" → true
version_ge() {
    local a="$1" b="$2"
    [ "$(printf '%s\n%s' "$a" "$b" | sort -V | head -n1)" = "$b" ]
}

get_python_version() {
    "$1" --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1
}

trap 'error "Installation failed at line $LINENO. See above for details."' ERR

# --- OS Detection ---

detect_os() {
    OS="unknown"
    DISTRO="unknown"
    PKG_MGR="unknown"
    ARCH="$(uname -m)"
    HAS_SUDO=0

    if command_exists sudo; then
        HAS_SUDO=1
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
                    nixos)
                        DISTRO="nixos"; PKG_MGR="nix" ;;
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

# --- NixOS helper ---

nixos_require() {
    local tool="$1"
    if command_exists "$tool"; then return 0; fi
    error "$tool is required but not found."
    error "On NixOS, enter the dev shell first:  nix develop"
    error "Or install $tool in your system/user profile."
    exit 1
}

# --- Dependency: git ---

ensure_git() {
    if command_exists git; then
        success "git $(git --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
        return
    fi

    if [ "$DISTRO" = "nixos" ]; then nixos_require git; return; fi

    info "git is not installed"
    if [ "$HAS_SUDO" = "0" ] && [ "$OS" = "linux" ]; then
        error "git is required but sudo is not available. Install git manually and re-run."
        exit 1
    fi

    if ! confirm "Install git?"; then
        error "git is required. Aborting."
        exit 1
    fi

    case "$PKG_MGR" in
        apt)
            sudo apt-get update -qq && sudo apt-get install -y -qq git ;;
        dnf)
            sudo dnf install -y -q git ;;
        pacman)
            sudo pacman -S --noconfirm git ;;
        zypper)
            sudo zypper install -y git ;;
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

    if [ "$DISTRO" = "nixos" ]; then nixos_require uv; return; fi

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

# --- Dependency: Python 3.12+ ---

ensure_python() {
    # Check for existing Python >= 3.12
    for candidate in python3.13 python3.12 python3; do
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

    # Fallback: try 3.12
    if uv python install "3.$MIN_PYTHON_MINOR" 2>/dev/null; then
        PYTHON_CMD="$(uv python find "3.$MIN_PYTHON_MINOR" 2>/dev/null || echo "")"
        if [ -n "$PYTHON_CMD" ]; then
            success "Python 3.$MIN_PYTHON_MINOR installed via uv"
            return
        fi
    fi

    # Last resort: system packages
    warn "uv python install failed. Trying system packages..."

    if [ "$DISTRO" = "nixos" ]; then
        error "uv python install failed. On NixOS, ensure you're in the dev shell: nix develop"
        exit 1
    fi

    if [ "$HAS_SUDO" = "0" ] && [ "$OS" = "linux" ]; then
        error "Cannot install Python: no sudo and uv python install failed."
        error "Install Python 3.12+ manually and re-run."
        exit 1
    fi

    case "$PKG_MGR" in
        apt)
            if ! apt-cache show python3.13 >/dev/null 2>&1; then
                info "Adding deadsnakes PPA..."
                sudo apt-get update -qq
                sudo apt-get install -y -qq software-properties-common
                sudo add-apt-repository -y ppa:deadsnakes/ppa
                sudo apt-get update -qq
            fi
            sudo apt-get install -y -qq python3.13 python3.13-venv python3.13-dev
            PYTHON_CMD="python3.13"
            ;;
        dnf)
            sudo dnf install -y -q python3.13 || sudo dnf install -y -q python3.12 || sudo dnf install -y -q python3
            PYTHON_CMD="$(command -v python3.13 || command -v python3.12 || command -v python3)"
            ;;
        pacman)
            sudo pacman -S --noconfirm python
            PYTHON_CMD="python3"
            ;;
        zypper)
            sudo zypper install -y python313 || sudo zypper install -y python312 || sudo zypper install -y python3
            PYTHON_CMD="$(command -v python3.13 || command -v python3.12 || command -v python3)"
            ;;
        brew)
            brew install python@3.13
            PYTHON_CMD="python3.13"
            ;;
        *)
            error "Don't know how to install Python on $DISTRO."
            error "Install Python 3.12+ manually and re-run."
            exit 1
            ;;
    esac

    if [ -z "${PYTHON_CMD:-}" ] || ! command_exists "$PYTHON_CMD"; then
        error "Failed to install Python. Install Python 3.12+ manually and re-run."
        exit 1
    fi

    success "Python $(get_python_version "$PYTHON_CMD") installed via system packages"
}

# --- Dependency: Node.js 18+ ---

ensure_node() {
    if command_exists node; then
        local ver
        ver="$(node --version | tr -d 'v' | cut -d. -f1)"
        if [ "$ver" -ge "$MIN_NODE_MAJOR" ] 2>/dev/null; then
            success "Node.js $(node --version)"
            return
        fi
        warn "Node.js $(node --version) is too old (need v${MIN_NODE_MAJOR}+)"
    fi

    if [ "$DISTRO" = "nixos" ]; then nixos_require node; return; fi

    info "Node.js ${MIN_NODE_MAJOR}+ is not installed"

    if [ "$HAS_SUDO" = "0" ] && [ "$OS" = "linux" ]; then
        error "Node.js is required but sudo is not available."
        error "Install Node.js ${MIN_NODE_MAJOR}+ manually and re-run."
        exit 1
    fi

    if ! confirm "Install Node.js?"; then
        error "Node.js is required for the web UI. Aborting."
        exit 1
    fi

    case "$PKG_MGR" in
        apt)
            info "Installing Node.js via nodesource..."
            curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
            sudo apt-get install -y -qq nodejs
            ;;
        dnf)
            info "Installing Node.js via nodesource..."
            curl -fsSL https://rpm.nodesource.com/setup_lts.x | sudo bash -
            sudo dnf install -y -q nodejs
            ;;
        pacman)
            sudo pacman -S --noconfirm nodejs npm
            ;;
        zypper)
            sudo zypper install -y nodejs20
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
            error "Install Node.js ${MIN_NODE_MAJOR}+ manually and re-run."
            exit 1
            ;;
    esac

    if ! command_exists node; then
        error "Node.js installation failed. Install manually and re-run."
        exit 1
    fi

    success "Node.js $(node --version) installed"
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

    if [ ! -d ".venv" ]; then
        info "Creating virtualenv..."
        uv venv --python "3.$PREFERRED_PYTHON_MINOR" 2>/dev/null \
            || uv venv --python "3.$MIN_PYTHON_MINOR" 2>/dev/null \
            || uv venv
    else
        info "Using existing virtualenv"
    fi

    info "Installing dependencies..."
    uv pip install -e . --quiet
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

run_init() {
    step "Running Nerve setup"

    local nerve_bin="$INSTALL_DIR/.venv/bin/nerve"
    local config_local="$INSTALL_DIR/config.local.yaml"

    if [ "$IS_UPGRADE" = "1" ] && [ -f "$config_local" ]; then
        info "Existing configuration found — skipping setup wizard"
        info "Run 'nerve init' to reconfigure"
        return
    fi

    cd "$INSTALL_DIR" || exit 1
    "$nerve_bin" init
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
    printf "  ${DIM}Config      : $INSTALL_DIR/config.local.yaml${NC}\n"
    printf "  ${DIM}Data        : ~/.nerve/${NC}\n"
    printf "\n"
    printf "  ${DIM}To uninstall: rm -rf $INSTALL_DIR ~/.nerve ~/.local/bin/nerve${NC}\n"
    printf "\n"

    if ! command_exists nerve; then
        warn "nerve is not yet on PATH in this shell session"
        printf "  Run: ${BOLD}source ~/.bashrc${NC}  (or restart your terminal)\n\n"
    fi
}

# --- Usage ---

usage() {
    cat <<EOF
Nerve Installer — https://github.com/ClickHouse/nerve

Usage:
  curl -fsSL https://raw.githubusercontent.com/ClickHouse/nerve/main/install.sh | bash
  curl -fsSL .../install.sh | bash -s -- --yes

Options:
  --yes, -y       Skip all confirmation prompts

Environment variables:
  NERVE_INSTALL_DIR   Where to clone (default: ~/nerve)
  NERVE_BRANCH        Git branch (default: main)
  NERVE_YES           Set to 1 to skip confirmations

EOF
}

# --- Main ---

main() {
    # Parse arguments
    for arg in "$@"; do
        case "$arg" in
            --yes|-y) AUTO_YES=1 ;;
            --help|-h) usage; exit 0 ;;
        esac
    done

    # When piped via curl | bash, stdin is the pipe (EOF after script).
    # Reclaim the terminal for all interactive prompts.
    if [ ! -t 0 ] && [ -e /dev/tty ]; then
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
    run_init
    print_summary
}

main "$@"
