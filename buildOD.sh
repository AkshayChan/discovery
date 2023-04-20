#!/usr/bin/env bash
set -e

# Build Operations Dashboard
cd ./operations_dashboard
# Cleanup the old build
rm -rf ./build

# Install packages
yarn #--force

# Get the stack name
if [[ -z "${DEPLOY_ENV}" ]]; then
  od_stack_name='dev-EurosportCyclingOD'
  consumption_stack_name='dev-EurosportCyclingConsumption'
else
  od_stack_name="${DEPLOY_ENV}-EurosportCyclingOD"
  consumption_stack_name="${DEPLOY_ENV}-EurosportCyclingConsumption"
fi

if [ -z "$1" ]
then
    echo "Collect real params for building"
    # Get the RESTAPIURL
    RESTAPIURL=$(aws cloudformation describe-stacks  --stack-name $consumption_stack_name --query "Stacks[].Outputs[?OutputKey=='GraphQlApiURL'].OutputValue" --output text)
    # # Get the REST API Sercret
    # KEYARN=$(aws cloudformation describe-stacks  --stack-name $stack_name --query "Stacks[].Outputs[?OutputKey=='GraphQlApiKey'].OutputValue" --output text)
    # SecretRef=$(echo ${KEYARN} | cut -c26-125)
    # APIKEY=$(aws secretsmanager get-secret-value --secret-id ${SecretRef} --query SecretString --output text | jq '.api_key')
    APIKEY=$(aws cloudformation describe-stacks  --stack-name $consumption_stack_name --query "Stacks[].Outputs[?OutputKey=='GraphQlApiKey'].OutputValue" --output text)

    USER_POOL_ID=$(aws cloudformation describe-stacks  --stack-name $od_stack_name --query "Stacks[].Outputs[?OutputKey=='UserPoolID'].OutputValue" --output text)
    CLIENT_ID=$(aws cloudformation describe-stacks  --stack-name $od_stack_name --query "Stacks[].Outputs[?OutputKey=='UserPoolWebClientID'].OutputValue" --output text)

    # Safe file into the proper folder
    cat <<EOF >>./src/aws-exports.js
const awsmobile =  {
    "aws_appsync_graphqlEndpoint": "${RESTAPIURL}",
    "aws_region": "eu-west-1",
    "aws_appsync_authenticationType": "API_KEY",
    "aws_appsync_apiKey": "${APIKEY}",
    "aws_cognito_userPoolId": "${USER_POOL_ID}",
    "aws_cognito_clientId": "${CLIENT_ID}",
};

export default awsmobile;

EOF
    # # Print the content. Temporarry
    # cat ./src/aws-exports.js

    # Run build process
    yarn build

    # Cleanup the config file
    rm ./src/aws-exports.js
else
    echo "CI, create only folder with 2 files"
    mkdir build
    # Create error.html
    cat <<EOF >>./build/index.html
<html xmlns="http://www.w3.org/1999/xhtml" >
<head>
    <title>Main Page</title>
</head>
<body>
  <p>Page example</p>
</body>
</html>

EOF
    # RESTAPIURL='https://1234567.execute-api.eu-west-1.amazonaws.com/dev/'
    # APIKEY='"1234567qDQITZHbk6dgDj562RG26wra."'

    # echo ${RESTAPIURL}
    # echo ${APIKEY}
fi

# Create error.html
cat <<EOF >>./build/error.html
<html xmlns="http://www.w3.org/1999/xhtml" >
<head>
    <title>Error Page</title>
</head>
<body>
  <p>Something went wrong</p>
</body>
</html>

EOF

# Show results
ls -l
ls -l ./build

# Exit from the folder
cd ..
