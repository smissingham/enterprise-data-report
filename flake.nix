{
  description = "Data Science Development Flake";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        python = pkgs.python312Full;

        # Add your dependencies here
        dependencies = with pkgs; [
          # Core System Utilities
          gcc
          stdenv.cc.cc.lib

          # Python dependencies
          uv
          python
        ];

        # Base shell hook that just sets up the environment on open
        baseEnvSetup = pkgs: ''
          # Run the full sync task to ensure env is set up and working
          ./scripts.sh --sync
        '';
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = dependencies;

          shellHook = ''
            ${baseEnvSetup pkgs}
          '';
        };
      }
    );
}
