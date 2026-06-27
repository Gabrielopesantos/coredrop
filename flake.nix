{
  description = "coredrop - standalone Kubernetes coredump handler";

  inputs.nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/0";

  outputs =
    { self, ... }@inputs:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forEachSupportedSystem =
        f:
        inputs.nixpkgs.lib.genAttrs supportedSystems (
          system:
          f {
            pkgs = import inputs.nixpkgs {
              inherit system;
              config = {
                allowUnfree = true;
                permittedInsecurePackages = [ "lima-1.2.2" ];
              };
            };
          }
        );
    in
    {
      devShells = forEachSupportedSystem (
        { pkgs }:
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              cargo
              rustc
              clippy
              rustfmt
              cargo-edit
              rust-analyzer
              bacon
              kubernetes-helm
              kubectl
              lima
              qemu
              jq
              podman
              minio-client # `mc`
              zstd
            ];
            env = {
              RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
            };
            shellHook = ''
              echo "dev shell"
              cargo --version
            '';
          };
        }
      );

      packages = forEachSupportedSystem (
        { pkgs }:
        {
          default =
            let
              pkg = (fromTOML (builtins.readFile ./Cargo.toml)).package;
            in
            pkgs.rustPlatform.buildRustPackage {
              pname = pkg.name;
              inherit (pkg) version;
              src = builtins.path { path = ./.; };
              cargoLock.lockFile = ./Cargo.lock;
            };
        }
      );

      formatter = forEachSupportedSystem ({ pkgs, ... }: pkgs.nixfmt);
    };
}
