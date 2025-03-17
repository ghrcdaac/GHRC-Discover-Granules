docker build . -t discover-granules &&
  CID=$(docker create discover-granules) &&
  package_name="ghrc_discover_granules_lambda.zip"
  docker cp "${CID}":/${package_name} ./${package_name} &&
  docker rm "${CID}" &&
  if [[ -n ${AWS_PROFILE} ]]; then
    aws --cli-connect-timeout 6000 lambda update-function-code --profile "${AWS_PROFILE}" --region=us-west-2 --function-name arn:aws:lambda:"${AWS_REGION}":"${AWS_ACCOUNT_ID}":function:"${PREFIX}"-ghrc-discover-granules --zip-file fileb://${package_name} --publish
  fi
