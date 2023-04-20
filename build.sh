#!/usr/bin/env bash
set -e

echo "Generate lambda functions"
cd ./lib/lambda/node/
npm install --production
npm run start
cd ../../..
echo "Lambdas build success"

echo "Generate Glue-job helper"
cd ./glue_jobs/glue_helper_libraries/
pip3 install wheel
python3 setup.py bdist_wheel
cd ../..
echo "Glue job helper library wheel file creation success"

# If param is not set, than it is human. Make simple tests with concol output
if [ -z "$1" ]
then
    echo "Create an empty build folder for OD stack"
    mkdir ./operations_dashboard/build
    echo "Install modules"
    npm i
    echo "Create all stacks"
    cdk synth
else
    echo "CI, testing in the config file. Exit"
fi

exit
