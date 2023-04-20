docker build . -t discover-granules &&
CID=$(docker create discover-granules) &&
docker cp "${CID}":/var/task/package/package.zip ./package.zip &&
docker rm "${CID}" &&
aws lambda update-function-code --profile WSBX --region=us-west-2 --function-name arn:aws:lambda:us-west-2:322322076095:function:sharedsbx-discover-granules-tf-module --zip-file fileb://package.zip --publish
