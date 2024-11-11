#!/usr/bin/env bash

# this is required if any git module has been initialised to another remote repo
git submodule sync && \
git submodule foreach "git fetch" && \
git submodule foreach "git reset --hard" && \
git submodule foreach "git config --unset core.fileMode || :"
## always use global fileMode, required for dual boot Windows/Linux (as git-bash on windows interpret NTFS permissions differently)

git submodule update --init --force