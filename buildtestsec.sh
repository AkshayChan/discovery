#!/usr/bin/env bash
set -e

echo 'Install modules for security testing'
# Install bandit
pip3 install bandit
# Require sudo, on mac. Quick solution use with sudo,

# If param is not set, than it is human. Make simple tests with concol output
if [ -z "$1" ]
then
    # Proper solution https://www.moncefbelyamani.com/how-to-install-xcode-homebrew-git-rvm-ruby-on-mac/?utm_source=stackoverflow
    gem install cfn-nag
    echo "Security testing for python"
    bandit -r ./
    echo "Security testing for Cloud formation"
    cfn_nag_scan -i ./cdk.out -t ..\*.template.json
else
    echo "CI, testing in the config file. Exit"
fi

exit
