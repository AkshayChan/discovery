#!/usr/bin/env bash
echo 'You are goint to cleanup your local environment.'
read -p "Are you sure? " -n 1 -r
echo    # (optional) move to a new line

if [[ $REPLY =~ ^[Yy]$ ]]
then
    # Folders
    rm -rf ./.pytest_cache/
    rm -rf ./cdk.out/
    rm -rf ./coverage/
    rm -rf ./glue_jobs/glue_helper_libraries/build/
    rm -rf ./glue_jobs/glue_helper_libraries/dist/
    rm -rf ./glue_jobs/glue_helper_libraries/glue_python_shell_helper_libraries.egg-info/
    rm -rf ./lib/lambda/node/node_modules/
    rm -rf ./lib/lambda/node/node_modules/
    rm -rf ./lib/lambda/node/api/dist/
    rm -rf ./lib/lambda/node/api/node_modules/
    rm -rf ./lib/lambda/node/ddb-resolver/dist/
    rm -rf ./lib/lambda/node/ddb-resolver/node_modules/
    rm -rf ./lib/lambda/node/invoke-mutation/dist/
    rm -rf ./lib/lambda/node/invoke-mutation/node_modules/
    rm -rf ./operations_dashboard/build/
    rm -rf ./operations_dashboard/node_modules/
    rm -rf ./node_modules/
    rm -rf ./types/dist/
    rm -rf ./types/node_modules/
    rm -rf ./test/__pycache__/
    # Files by mask
    rm ./bin/*.d.ts
    rm ./bin/*.js
    rm ./lib/*.d.ts
    rm ./lib/*.js
    rm ./test/*.d.ts
    rm ./test/*.js
    # Single files
    rm .coverage
    rm dev.json
    rm test.json
    rm package-lock.json
    rm test-report.xml
    rm ./operations_dashboard/tsconfig.json
    rm ./operations_dashboard/src/aws-exports.js
    rm ./operations_dashboard/package-lock.json
    rm ./types/graphql.schema.json
else
    echo "Canceled!"
fi

exit
