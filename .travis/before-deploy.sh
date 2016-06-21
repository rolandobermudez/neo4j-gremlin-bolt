#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    openssl aes-256-cbc -K $encrypted_5efcbdf945b5_key -iv $encrypted_5efcbdf945b5_iv -in .travis/codesigning.asc.enc -out .travis/codesigning.asc -d
    gpg --fast-import .travis/signingkey.asc
fi

exit 0
