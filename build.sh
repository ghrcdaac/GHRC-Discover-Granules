docker build . -t discover-granules &&
  CID=$(docker create discover-granules) &&
  docker cp "${CID}":/var/task/package/package.zip ./package.zip &&
  docker rm "${CID}" &&
  if [[  -n $AWS_PROFILE ]]; then
    aws lambda update-function-code \
    --profile "${AWS_PROFILE}" --region=us-west-2 \
    --function-name arn:aws:lambda:"${AWS_REGION}":"${AWS_ACCOUNT_NUMBER}":function:"${PREFIX}"-ghrc-discover-granules \
    --zip-file fileb://package.zip --publish
  fi
