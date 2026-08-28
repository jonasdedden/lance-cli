# Install

## Prebuilt binary

```sh
uv tool install lance-cli
```

The PyPI package `lance-cli` ships the `lance-cli` binary; `pipx install
lance-cli` works the same way.

## From crates.io

```sh
cargo install lance-cli
```

The crate and the binary are named `lance-cli`; the library is `lance_cli`.

## From a clone

```sh
git clone https://github.com/jonasdedden/lance-cli
cd lance-cli
cargo install --path .

# Or run without installing:
cargo run --release -- head -n 5 dataset.lance
```

The repository uses [`just`](https://github.com/casey/just) for common tasks:
`just check` runs formatting, clippy, and the test suite; `just wheel` and
`just sdist` build the Python distributions.

## Shell completions

`lance-cli completions <shell>` writes a completion script to stdout for `bash`,
`zsh`, `fish`, `powershell`, and `elvish`. Put it where your shell looks for
completions:

```sh
# bash
lance-cli completions bash | sudo tee /etc/bash_completion.d/lance-cli > /dev/null

# zsh — into a directory on $fpath, with `fpath+=(~/.zfunc)` before `compinit`
lance-cli completions zsh > ~/.zfunc/_lance-cli

# fish
lance-cli completions fish > ~/.config/fish/completions/lance-cli.fish

# PowerShell — write a file and dot-source it from your profile
lance-cli completions powershell > $HOME\lance-cli.completion.ps1
Add-Content $PROFILE '. $HOME\lance-cli.completion.ps1'
```
