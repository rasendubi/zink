{ pkgs ? import <nixpkgs> { }
}:
with pkgs;
stdenv.mkDerivation {
  name = "zinc-0.1";

  buildInputs = [
    rustc
    cargo
  ];
}
