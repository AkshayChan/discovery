#!/usr/bin/env bash
set -e

#echo 'Execute the build first'
#bash build.sh

echo "Install modules for static code testing"
npm install --only=dev

echo "Install modules for python testing"
pip3 install -r requirements.txt
# Unit tests for TypeScript
# npm i --save-dev jest jest-junit @types/jest @aws-cdk/assert
# Linting for Typescript
# npm i --save-dev eslint eslint-config-airbnb eslint-plugin-import @typescript-eslint/parser @typescript-eslint/eslint-plugin

# If param is not set, than it is human. Make simple tests with concol output
if [ -z "$1" ]
then
    echo "Static code analysis for Typescript"
    npx eslint ./bin/*.ts
    npx eslint ./lib/*.ts
    echo "Unit tests for Typescript"
    npx jest
    # Python block
    echo "Static code analysis for Python"
    pylint lib
    echo "Unit tests for Python"
    coverage run -m pytest --cov=lib
else
    echo "CI, testing in the config file. Exit"
fi

exit

