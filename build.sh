docker build . -t discover-granules &&
CID=$(docker create discover-granules) &&
docker cp "${CID}":/var/task/package/package.zip ./package.zip &&
docker rm "${CID}" &&

