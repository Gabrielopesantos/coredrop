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
              helm-docs
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
        let
          pkg = (fromTOML (builtins.readFile ./Cargo.toml)).package;
          # Static (musl) build: the daemon installs this binary onto the node
          # and the kernel exec's it as the core_pattern handler in the host
          # mount namespace, where the nix store does not exist - so it must be
          # fully self-contained, not dynamically linked against /nix/store.
          coredrop = pkgs.pkgsStatic.rustPlatform.buildRustPackage {
            pname = pkg.name;
            inherit (pkg) version;
            src = builtins.path { path = ./.; };
            cargoLock.lockFile = ./Cargo.lock;
          };
        in
        {
          default = coredrop;

          # Release container image, published to ghcr.io on `app-v*` tags. Built
          # natively per system (x86_64 / aarch64); CI stitches both into one
          # multi-arch manifest. cacert covers object-store TLS. No crictl here:
          # the kernel-exec'd handler runs in the host mount ns and shells out to
          # the *node's* crictl (cri.crictlPath), not the image's.
          image = pkgs.dockerTools.buildLayeredImage {
            name = "coredrop";
            contents = [
              (pkgs.buildEnv {
                name = "coredrop-root";
                paths = [
                  coredrop
                  pkgs.cacert
                  # busybox supplies /bin/sh + coreutils (install) for the
                  # chart's install-handler initContainer.
                  pkgs.busybox
                ];
                pathsToLink = [
                  "/bin"
                  "/etc"
                ];
              })
            ];
            config = {
              Entrypoint = [ "/bin/coredrop" ];
              Env = [
                "PATH=/bin"
                "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
              ];
            };
          };
        }
      );

      formatter = forEachSupportedSystem ({ pkgs, ... }: pkgs.nixfmt);
    };
}
