{ pkgs ? import <nixpkgs> { }
}:
with pkgs;
rustPlatform.buildRustPackage {
  name = "zinc-0.1.1";

  src = ./.;

  depsSha256 = "1j7mipqd1n146xds8136c9dq87af821yfw4qk3m40531m9zw4pi4";

  # buildInputs = [
  #   rustc
  #   cargo
  # ];
}
