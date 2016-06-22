#!/usr/bin/env bash

if [ "$TRAVIS_BRANCH" = 'master' ] && [ "$TRAVIS_PULL_REQUEST" == 'false' ]; then
    # feedback
    echo "Deploying artifacts..."
    # deploy
    mvn deploy -P sign,build-extras --settings .travis/mvnsettings.xml -Dgpg.passphrase=$GPG_PASSPHRASE
fi

exit 0
