# Build
You need rustc and cargo (default tooling for Rust). Folow [the instructions](https://www.rust-lang.org/en-US/install.html).

If you have [Nix](http://nixos.org/nix/) installed, you can get all dependencies using `nix-shell`.

After dependencies are installed, execute the following command.
```sh
cargo build
```

# Run
Use the next command to run Zink.
```sh
cargo run -- [-f/--file <file to append results to>] jsonpaths
```

If file is not specified, result is appended to stdout.

## Example
Imagine device sending the following data to the Zink:
```json
[
  {"time": 10, "temp": 16, "bat": 28},
  {"bat": 27.9, "temp": 16.25, "time": 11}
]
```

You can extract it into CSV using the following call.
```sh
cargo run -- time,bat,temp
```

The result is next.
```
10,28,16
11,27.9,16.25
```
