{
  description = "Nerve — self-hosted AI agent runtime";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            # Core tools
            uv
            nodejs_22
            git

            # Native libs for Python wheel builds (bcrypt, cryptography, aiosqlite, etc.)
            openssl
            sqlite
            libffi
            pkg-config
          ];

          env = {
            # Let uv manage Python — don't pull it from Nix
            UV_PYTHON_PREFERENCE = "only-managed";
          };

          shellHook = ''
            # Help native builds find nix-provided libs
            export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath [ pkgs.openssl pkgs.sqlite pkgs.libffi ]}:$LD_LIBRARY_PATH"
          '';
        };
      });
}
